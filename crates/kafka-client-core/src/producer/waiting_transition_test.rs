//! Flush membership and terminal ordering for accepted waiting operations.

use crate::{
    ByteCount, Deadline, ExplicitRecord, FlushId, Moment, OperationId, PartitionIndex, PayloadId,
    ProducerBatchPolicy, ProducerCancellationOutcome, ProducerCompletion, ProducerEffect,
    ProducerFailureKind, ProducerInput, ProducerMachine, ProducerMachineError,
    ProducerWaitingTerminal, TopicId,
};

const BYTES: ByteCount = ByteCount::new(11);

#[test]
fn waiting_terminals_publish_before_their_flush_barrier() {
    for (terminal, expected) in [
        (
            ProducerWaitingTerminal::Cancelled,
            ProducerFailureKind::Cancelled,
        ),
        (
            ProducerWaitingTerminal::DeadlineElapsed,
            ProducerFailureKind::DeadlineElapsed,
        ),
        (
            ProducerWaitingTerminal::Closed,
            ProducerFailureKind::ExecutionUnavailable,
        ),
        (
            ProducerWaitingTerminal::MetadataUnavailable {
                broker_code: Some(3),
            },
            ProducerFailureKind::Routing,
        ),
    ] {
        let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
        let operation_id = admit_waiting(&mut producer);
        let flush_id = request_flush(&mut producer);

        let settled = producer
            .apply(ProducerInput::WaitingTerminal {
                operation_id,
                terminal,
            })
            .unwrap_or_else(|error| panic!("waiting terminal failed: {error}"));

        let [
            ProducerEffect::Complete {
                operation_id: completed,
                completion: ProducerCompletion::Failed(failure),
            },
            ProducerEffect::CompleteFlush {
                flush_id: completed_flush,
            },
        ] = settled.effects()
        else {
            panic!("waiting terminal must precede its newly-ready flush: {settled:?}")
        };
        assert_eq!(*completed, operation_id);
        assert_eq!(failure.kind(), expected);
        assert_eq!(*completed_flush, flush_id);
    }
}

#[test]
fn promotion_retains_the_waiting_identity_and_prior_flush_membership() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let operation_id = admit_waiting(&mut producer);
    let flush_id = request_flush(&mut producer);

    let promoted = producer
        .apply(ProducerInput::PromoteWaiting {
            operation_id,
            now: Moment::from_tick(1),
            record: record(),
        })
        .unwrap_or_else(|error| panic!("waiting promotion failed: {error}"));
    assert_eq!(promoted.admitted_operation_id(), Some(operation_id));
    let cancelled = producer
        .apply(ProducerInput::CancelRequested { operation_id })
        .unwrap_or_else(|error| panic!("promoted cancellation failed: {error}"));

    assert_eq!(
        cancelled.cancellation_outcome(),
        Some(ProducerCancellationOutcome::CancelledNotSent)
    );
    assert_eq!(
        cancelled.effects().last(),
        Some(&ProducerEffect::CompleteFlush { flush_id })
    );
}

#[test]
fn close_barrier_includes_waiting_operations_accepted_before_it() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let operation_id = admit_waiting(&mut producer);
    let close = producer
        .apply(ProducerInput::CloseRequested)
        .unwrap_or_else(|error| panic!("close barrier failed: {error}"));
    let Some(ProducerEffect::AcceptFlush { flush_id, .. }) = close.effects().first() else {
        panic!("close must accept one drain barrier")
    };
    assert_eq!(close.effects().len(), 1);

    let terminal = producer
        .apply(ProducerInput::WaitingTerminal {
            operation_id,
            terminal: ProducerWaitingTerminal::Closed,
        })
        .unwrap_or_else(|error| panic!("closed waiting terminal failed: {error}"));
    assert_eq!(
        terminal.effects().last(),
        Some(&ProducerEffect::CompleteFlush {
            flush_id: *flush_id,
        })
    );
}

#[test]
fn flush_excludes_waiting_operation_accepted_after_its_barrier() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 3);
    let included = admit_waiting(&mut producer);
    let flush_id = request_flush(&mut producer);
    let excluded = admit_waiting(&mut producer);

    let settled = producer
        .apply(ProducerInput::WaitingTerminal {
            operation_id: included,
            terminal: ProducerWaitingTerminal::Cancelled,
        })
        .unwrap_or_else(|error| panic!("included waiting terminal failed: {error}"));

    assert_eq!(
        settled.effects().last(),
        Some(&ProducerEffect::CompleteFlush { flush_id })
    );
    assert_ne!(included, excluded);
    assert_eq!(producer.completion_slots(), 2);
}

#[test]
fn overlapping_flushes_wait_for_out_of_order_waiting_terminals() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 4);
    let first = admit_waiting(&mut producer);
    let first_flush = request_flush(&mut producer);
    let second = admit_waiting(&mut producer);
    let second_flush = request_flush(&mut producer);

    let later = producer
        .apply(ProducerInput::WaitingTerminal {
            operation_id: second,
            terminal: ProducerWaitingTerminal::DeadlineElapsed,
        })
        .unwrap_or_else(|error| panic!("later waiting terminal failed: {error}"));
    assert!(
        later
            .effects()
            .iter()
            .all(|effect| !matches!(effect, ProducerEffect::CompleteFlush { .. }))
    );

    let earlier = producer
        .apply(ProducerInput::WaitingTerminal {
            operation_id: first,
            terminal: ProducerWaitingTerminal::Cancelled,
        })
        .unwrap_or_else(|error| panic!("earlier waiting terminal failed: {error}"));
    assert_eq!(
        &earlier.effects()[earlier.effects().len() - 2..],
        [
            ProducerEffect::CompleteFlush {
                flush_id: first_flush,
            },
            ProducerEffect::CompleteFlush {
                flush_id: second_flush,
            },
        ]
    );
}

#[test]
fn promotion_backpressure_retains_identity_and_prior_flush_membership() {
    let policy = ProducerBatchPolicy::try_new(2, ByteCount::new(64), 100)
        .unwrap_or_else(|error| panic!("two-record policy failed: {error}"));
    let mut producer = ProducerMachine::with_batch_policy_and_flush_capacity(BYTES, 3, policy, 2);
    let active = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(10),
            record: record_with_payload(2),
        })
        .unwrap_or_else(|error| panic!("active admission failed: {error}"))
        .admitted_operation_id()
        .unwrap_or_else(|| panic!("active operation identity"));
    let waiting = admit_waiting(&mut producer);
    let flush_id = request_flush(&mut producer);

    assert_eq!(
        producer.apply(ProducerInput::PromoteWaiting {
            operation_id: waiting,
            now: Moment::from_tick(1),
            record: record(),
        }),
        Err(ProducerMachineError::Admission(
            crate::AdmissionRejection::ByteCapacity,
        ))
    );
    let active_cancelled = producer
        .apply(ProducerInput::CancelRequested {
            operation_id: active,
        })
        .unwrap_or_else(|error| panic!("active cancellation failed: {error}"));
    assert!(
        active_cancelled
            .effects()
            .iter()
            .all(|effect| !matches!(effect, ProducerEffect::CompleteFlush { .. }))
    );

    let promoted = producer
        .apply(ProducerInput::PromoteWaiting {
            operation_id: waiting,
            now: Moment::from_tick(2),
            record: record(),
        })
        .unwrap_or_else(|error| panic!("waiting retry failed: {error}"));
    assert_eq!(promoted.admitted_operation_id(), Some(waiting));
    let waiting_cancelled = producer
        .apply(ProducerInput::CancelRequested {
            operation_id: waiting,
        })
        .unwrap_or_else(|error| panic!("promoted waiting cancellation failed: {error}"));
    assert_eq!(
        waiting_cancelled.effects().last(),
        Some(&ProducerEffect::CompleteFlush { flush_id })
    );
}

fn admit_waiting(producer: &mut ProducerMachine) -> OperationId {
    producer
        .apply(ProducerInput::AdmitWaiting {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(10),
            retained_bytes: BYTES,
        })
        .unwrap_or_else(|error| panic!("waiting admission failed: {error}"))
        .admitted_operation_id()
        .unwrap_or_else(|| panic!("waiting admission must allocate one stable operation"))
}

fn request_flush(producer: &mut ProducerMachine) -> FlushId {
    let transition = producer
        .apply(ProducerInput::FlushRequested)
        .unwrap_or_else(|error| panic!("flush failed: {error}"));
    let Some(ProducerEffect::AcceptFlush { flush_id, .. }) = transition.effects().first() else {
        panic!("flush must retain the waiting operation")
    };
    assert_eq!(transition.effects().len(), 1);
    *flush_id
}

const fn record() -> ExplicitRecord {
    record_with_payload(1)
}

const fn record_with_payload(payload: u64) -> ExplicitRecord {
    ExplicitRecord::new(
        PayloadId::from_raw(payload),
        TopicId::from_raw(1),
        PartitionIndex::from_raw(0),
        BYTES,
    )
}
