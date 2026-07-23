//! Tests for producer operation ownership stages.

use crate::{
    BatchId, ByteCount, Deadline, DeliveryStatus, OperationId, ProducerOperation,
    ProducerOperationState, TransitionError,
};

const BATCH: BatchId = BatchId::from_raw(7);

fn operation() -> ProducerOperation {
    ProducerOperation::new(
        OperationId::from_raw(1),
        Deadline::from_tick(100),
        ByteCount::new(64),
    )
}

#[test]
fn operation_records_materialization_before_driver_ownership() {
    let mut operation = operation();
    assert_eq!(operation.admit(BATCH), Ok(()));
    assert_eq!(operation.mark_ready(BATCH), Ok(()));
    assert!(matches!(
        operation.state(),
        ProducerOperationState::Materializing {
            batch_id: BATCH,
            ..
        }
    ));
    assert_eq!(operation.mark_materialized(BATCH), Ok(()));
    assert!(matches!(
        operation.state(),
        ProducerOperationState::AwaitingDriver {
            batch_id: BATCH,
            ..
        }
    ));
}

#[test]
fn driver_acceptance_does_not_invent_delivery_certainty() {
    let mut operation = operation();
    assert_eq!(operation.admit(BATCH), Ok(()));
    assert_eq!(operation.mark_ready(BATCH), Ok(()));
    assert_eq!(operation.mark_materialized(BATCH), Ok(()));
    assert_eq!(operation.mark_submitted(BATCH), Ok(()));
    assert_eq!(
        operation.plan_failed(DeliveryStatus::NotSent),
        Ok(crate::TerminalRelease {
            released_bytes: Some(ByteCount::new(64)),
        })
    );
}

#[test]
fn possibly_sent_is_invalid_before_driver_ownership() {
    let mut operation = operation();
    assert_eq!(operation.admit(BATCH), Ok(()));
    assert_eq!(
        operation.plan_failed(DeliveryStatus::PossiblySent),
        Err(TransitionError::InvalidState)
    );
}

#[test]
fn mismatched_batch_cannot_advance_operation() {
    let mut operation = operation();
    assert_eq!(operation.admit(BATCH), Ok(()));
    assert_eq!(
        operation.mark_ready(BatchId::from_raw(8)),
        Err(TransitionError::BatchMismatch)
    );
}
