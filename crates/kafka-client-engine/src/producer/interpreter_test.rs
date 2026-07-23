//! Effect ordering, timer expiry, release, and terminal publication scenarios.

use std::collections::VecDeque;

use kafka_client_core::{
    BatchId, BatchTimerGeneration, ByteCount, Deadline, FlushId, Moment, OperationId,
    ProducerBatchPolicy, ProducerCompletion, ProducerEffect, ProducerInput, ProducerMachineError,
};

use crate::{
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    completion::CompletionRegistryError,
};

use super::{
    ProducerHostInvariantError, ProducerHostLimits,
    admission::ProducerAdmissionFailure,
    admission_test::{admit, record},
    effect::FailedEffectDisposition,
    host_limits_test::{start, valid_limits},
};

#[test]
fn generated_accumulation_waits_until_the_whole_admission_transition_drains() {
    let Ok(batch_policy) = ProducerBatchPolicy::try_new(1, ByteCount::new(u64::MAX), 10) else {
        panic!("test policy should be valid")
    };
    let limits = ProducerHostLimits {
        retained_bytes: 64,
        completion_capacity: 1,
        record_capacity: 1,
        batch_capacity: 1,
        timer_capacity: 1,
        notification_capacity: 1,
        encoded_byte_capacity: 1_024,
        max_wire_batch_bytes: 1_024,
        batch_policy,
    };
    let mut host = start(limits);
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(50),
        record("orders"),
    );

    assert_eq!(host.stats().active_timers, 0);
    assert_eq!(host.stats().pending_effects, 1);
    assert!(matches!(
        host.pending_effects(),
        [ProducerEffect::MaterializeBatch { .. }]
    ));
    assert_eq!(host.retry_terminal_backlog(1), Ok(0));
    assert_eq!(host.stats().pending_effects, 1);
    drop(admitted);
}

#[test]
fn due_deadline_releases_engine_bytes_before_publishing_terminal_failure() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let operation_id = admitted.operation_id();
    let observer = admitted.into_delivery_observer();

    assert_eq!(host.fire_due(Moment::from_tick(5), 1), Ok(1));
    let stats = host.stats();
    assert_eq!(stats.store.records, 0);
    assert_eq!(stats.store.bytes, 0);
    assert_eq!(stats.store.batches, 0);
    assert_eq!(stats.core_retained_bytes, ByteCount::new(0));
    assert_eq!(stats.core_completion_slots, 1);
    assert_eq!(stats.active_timers, 0);
    assert_eq!(stats.pending_effects, 0);

    let Err(ProducerDeliveryError::Failed(failure)) = observer.wait() else {
        panic!("deadline should fail the producer operation")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
    assert_eq!(operation_id.get(), 1);
}

#[test]
fn notifier_stop_retains_every_same_transition_terminal_in_exact_fifo_order() {
    let mut host = start(valid_limits());
    let first = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    let second = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    let transition = host
        .core
        .apply(ProducerInput::ExecutionUnavailable)
        .unwrap_or_else(|error| panic!("execution stop should plan: {error}"));
    let recovery = host.recover_notifier();

    assert_eq!(
        host.interpret_transition(Moment::from_tick(1), transition),
        Err(ProducerHostInvariantError::Completion(
            CompletionRegistryError::NotifierStopped
        ))
    );
    assert_eq!(host.stats().terminal_backlog, 2);
    assert_eq!(
        host.terminal_front()
            .map(super::terminal_backlog::RetainedTerminal::operation_id),
        Some(first.operation_id())
    );
    assert_eq!(
        host.terminal_back()
            .map(super::terminal_backlog::RetainedTerminal::operation_id),
        Some(second.operation_id())
    );
    assert_eq!(host.terminal_publish_attempts(), 1);

    drop((first, second));
    recovery
        .notifier
        .unwrap_or_else(|| panic!("notifier ownership must remain recoverable"))
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier should join: {error}"));
}

#[test]
fn generated_fact_and_failing_current_mechanism_remain_owned_after_poison() {
    let mut host = start(valid_limits());
    fill_timer_capacity(&mut host);
    let failure = host
        .try_admit_explicit(
            Moment::from_tick(0),
            Deadline::from_tick(100),
            record("orders"),
        )
        .err()
        .unwrap_or_else(|| panic!("timer saturation must poison accepted admission"));
    let (error, observer) = accepted_failure(failure);

    assert!(matches!(error, ProducerHostInvariantError::Timer(_)));
    assert!(matches!(
        host.terminal_quarantine.generated(),
        Some([ProducerInput::RecordAccumulated { .. }])
    ));
    assert!(matches!(
        host.terminal_quarantine.committed_tail(),
        Some([ProducerEffect::ArmBatchTimer { .. }])
    ));
    assert!(host.execution_unavailable(Moment::from_tick(1)).is_err());
    drop(observer);
    assert!(host.terminal_resources_empty());
}

#[test]
fn generated_capacity_refusal_owns_the_refused_fact_and_remaining_effect() {
    let mut host = start(valid_limits());
    host.effect_capacity = 0;
    let failure = host
        .try_admit_explicit(
            Moment::from_tick(0),
            Deadline::from_tick(100),
            record("orders"),
        )
        .err()
        .unwrap_or_else(|| panic!("zero test fact capacity must poison admission"));
    let (error, observer) = accepted_failure(failure);

    assert_eq!(error, ProducerHostInvariantError::GeneratedFactCapacity);
    assert!(matches!(
        host.terminal_quarantine.generated(),
        Some([ProducerInput::RecordAccumulated { .. }])
    ));
    assert!(matches!(
        host.terminal_quarantine.committed_tail(),
        Some([ProducerEffect::ArmBatchTimer { .. }])
    ));
    assert!(host.execution_unavailable(Moment::from_tick(1)).is_err());
    drop(observer);
    assert!(host.terminal_resources_empty());
}

#[test]
fn failed_mechanism_disposition_owns_the_effect_inline() {
    let mut host = start(valid_limits());
    let expected = ProducerEffect::CompleteFlush {
        flush_id: FlushId::from_raw(7),
    };
    let Err(failure) = host.interpret_effect_owned(Moment::from_tick(0), expected) else {
        panic!("unsupported flush must return its exact effect")
    };

    assert!(matches!(
        failure.into_parts().1,
        FailedEffectDisposition::Mechanism { effect, .. } if effect == expected
    ));
}

#[test]
fn core_rejection_retains_the_current_and_remaining_generated_fifo() {
    let mut host = start(valid_limits());
    let current = ProducerInput::RecordAccumulated {
        operation_id: OperationId::from_raw(99),
        batch_id: BatchId::from_raw(88),
        accumulator_bytes: ByteCount::new(7),
        now: Moment::from_tick(3),
    };
    let queued = ProducerInput::ExecutionUnavailable;
    let mut generated = VecDeque::from([current, queued]);

    assert_eq!(
        host.drain_generated(Moment::from_tick(3), &mut generated),
        Err(ProducerHostInvariantError::Core(
            ProducerMachineError::UnknownOperation
        ))
    );
    assert!(generated.is_empty());
    assert_eq!(
        host.terminal_quarantine.generated(),
        Some([current, queued].as_slice())
    );
    assert_eq!(host.fatal_transition.retained_len(), 0);
}

#[test]
fn failing_current_and_mixed_tail_are_owned_before_poison_fences_reentry() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    let transition = host
        .core
        .apply(ProducerInput::ExecutionUnavailable)
        .unwrap_or_else(|error| panic!("execution stop should plan: {error}"));
    let repeated = transition.clone();
    host.store.clear_terminal();

    let first = host.interpret_transition(Moment::from_tick(1), transition);
    assert!(matches!(
        first,
        Err(ProducerHostInvariantError::Prepared(_))
    ));
    assert_eq!(host.stats().terminal_backlog, 1);
    assert!(matches!(
        host.terminal_quarantine.committed_tail(),
        Some([
            ProducerEffect::ReleaseBatch { .. },
            ProducerEffect::ReleasePayload { .. }
        ])
    ));
    let retained = host.terminal_quarantine.retained_len();
    assert_eq!(
        host.interpret_transition(Moment::from_tick(2), repeated),
        first
    );
    assert_eq!(host.terminal_quarantine.retained_len(), retained);
    assert_eq!(host.fatal_transition.retained_len(), 0);

    assert!(host.execution_unavailable(Moment::from_tick(2)).is_err());
    assert!(admitted.into_delivery_observer().wait().is_err());
    assert!(host.terminal_resources_empty());
}

fn fill_timer_capacity(host: &mut super::ProducerHost) {
    for raw in [90, 91] {
        host.timers
            .arm(
                BatchId::from_raw(raw),
                BatchTimerGeneration::from_raw(1),
                Deadline::from_tick(500),
            )
            .unwrap_or_else(|error| panic!("test timer should fill capacity: {error}"));
    }
}

fn accepted_failure(
    failure: ProducerAdmissionFailure,
) -> (
    ProducerHostInvariantError,
    crate::completion::CompletionObserver<ProducerCompletion>,
) {
    let ProducerAdmissionFailure::AcceptedInvariant(poisoned) = failure else {
        panic!("post-core poison must retain the accepted observer")
    };
    let (error, _operation_id, observer) = poisoned.into_parts();
    (error, observer)
}
