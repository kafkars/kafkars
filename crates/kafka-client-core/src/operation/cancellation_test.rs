//! Operation cancellation preflight is stage-aware and restartable.

use crate::{
    BatchId, ByteCount, Deadline, DeliveryStatus, OperationId, ProducerOperation,
    ProducerOperationState, TransitionError,
};

#[test]
fn cancellation_is_owned_only_before_submission() {
    let batch_id = BatchId::from_raw(7);
    let mut operation = ProducerOperation::new(
        OperationId::from_raw(1),
        Deadline::from_tick(10),
        ByteCount::new(4),
    );
    operation
        .admit(batch_id)
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    assert_eq!(
        operation
            .plan_cancel()
            .map(crate::TerminalRelease::released_bytes),
        Ok(Some(ByteCount::new(4)))
    );
    operation
        .mark_ready(batch_id)
        .unwrap_or_else(|error| panic!("ready transition failed: {error}"));
    operation
        .mark_materialized(batch_id)
        .unwrap_or_else(|error| panic!("materialization transition failed: {error}"));
    assert!(operation.plan_cancel().is_ok());
    operation
        .mark_submitted(batch_id)
        .unwrap_or_else(|error| panic!("submission transition failed: {error}"));
    assert_eq!(operation.plan_cancel(), Err(TransitionError::InvalidState));
    assert!(operation.plan_failed(DeliveryStatus::PossiblySent).is_ok());
}

#[test]
fn execution_restart_returns_awaiting_driver_to_materializing() {
    let batch_id = BatchId::from_raw(7);
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
        .require_execution_restart(batch_id)
        .unwrap_or_else(|error| panic!("restart preflight failed: {error}"));
    operation.commit_execution_restart(batch_id);

    assert!(matches!(
        operation.state(),
        ProducerOperationState::Materializing { .. }
    ));
}
