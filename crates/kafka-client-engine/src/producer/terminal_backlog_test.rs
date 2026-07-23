//! Exact terminal retention through FIFO and interpreter-capacity failures.

use kafka_client_core::{
    BatchId, BatchTimerGeneration, Deadline, DeliveryStatus, Moment, OperationId,
    ProducerCompletion, ProducerEffect, ProducerFailure, ProducerInput,
};

use crate::{clock::OperationDeadline, completion::CompletionId};

use super::{
    ProducerHost, ProducerHostInvariantError,
    admission::ProducerAdmissionFailure,
    admission_test::record,
    host_limits_test::{start, valid_limits},
    terminal_backlog::{OrderedTerminalBacklog, RetainedTerminal},
};

#[test]
fn record_terminals_leave_the_bounded_fifo_only_from_the_front() {
    let mut backlog = OrderedTerminalBacklog::new(2);
    backlog.push(retained(1, 0));
    backlog.push(retained(2, 1));

    assert_operation(backlog.front(), 1);
    let Some(first) = backlog.pop_published() else {
        panic!("first terminal should remain owned")
    };
    assert_operation(Some(&first), 1);
    assert_operation(backlog.front(), 2);
    assert_eq!(backlog.len(), 1);
}

#[test]
fn generated_fact_and_failing_current_mechanism_remain_owned_after_poison() {
    let mut host = start(valid_limits());
    fill_timer_capacity(&mut host);
    let failure = host
        .try_admit_explicit(Moment::from_tick(0), deadline(), record("orders"))
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
        .try_admit_explicit(Moment::from_tick(0), deadline(), record("orders"))
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

fn retained(operation: u64, slot: usize) -> RetainedTerminal {
    RetainedTerminal::new(
        OperationId::from_raw(operation),
        CompletionId::from_parts_for_test(slot, 1),
        terminal(),
    )
}

fn terminal() -> ProducerCompletion {
    ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
        DeliveryStatus::NotSent,
    ))
}

fn assert_operation(terminal: Option<&RetainedTerminal>, expected: u64) {
    let Some(terminal) = terminal else {
        panic!("backlog must contain one exact record terminal")
    };
    assert_eq!(terminal.operation_id().get(), expected);
}

fn fill_timer_capacity(host: &mut ProducerHost) {
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

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(100), std::time::Instant::now())
}
