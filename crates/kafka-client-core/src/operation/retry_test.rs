//! Operation retry stages preserve original resource and deadline ownership.

use crate::{BatchId, ByteCount, Deadline, OperationId, ProducerOperation, ProducerOperationState};

#[test]
fn definitely_unsent_driver_owned_operation_waits_then_rematerializes() {
    let batch_id = BatchId::from_raw(7);
    let mut operation = awaiting_driver(batch_id);

    operation.commit_retry_waiting(batch_id);

    assert_eq!(
        operation.state(),
        ProducerOperationState::RetryWaiting {
            deadline: Deadline::from_tick(10),
            bytes: ByteCount::new(4),
            batch_id,
        }
    );
    operation
        .require_retry_waiting(batch_id)
        .unwrap_or_else(|error| panic!("retry-wait preflight failed: {error}"));
    operation.commit_retry_ready(batch_id);
    assert_eq!(
        operation.state(),
        ProducerOperationState::Materializing {
            deadline: Deadline::from_tick(10),
            bytes: ByteCount::new(4),
            batch_id,
        }
    );
}

#[test]
fn definitely_unsent_submitted_operation_becomes_safely_cancellable() {
    let batch_id = BatchId::from_raw(7);
    let mut operation = awaiting_driver(batch_id);
    operation
        .mark_submitted(batch_id)
        .unwrap_or_else(|error| panic!("submission transition failed: {error}"));
    operation
        .require_submitted(batch_id)
        .unwrap_or_else(|error| panic!("submitted preflight failed: {error}"));

    operation.commit_retry_waiting(batch_id);

    assert!(operation.plan_cancel().is_ok());
}

fn awaiting_driver(batch_id: BatchId) -> ProducerOperation {
    let mut operation = ProducerOperation::admitted(
        OperationId::from_raw(1),
        Deadline::from_tick(10),
        ByteCount::new(4),
        batch_id,
    );
    operation
        .mark_ready(batch_id)
        .unwrap_or_else(|error| panic!("ready transition failed: {error}"));
    operation
        .mark_materialized(batch_id)
        .unwrap_or_else(|error| panic!("materialization transition failed: {error}"));
    operation
}
