//! Terminal certainty and byte-release scenarios for operation ownership.

use crate::{
    BatchId, ByteCount, Deadline, DeliveryStatus, OperationId, ProducerOperation, TransitionError,
};

#[test]
fn retry_waiting_releases_bytes_only_with_definitely_unsent_evidence() {
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
    operation.commit_retry_waiting(batch_id);

    assert_eq!(
        operation
            .plan_failed(DeliveryStatus::NotSent)
            .map(crate::TerminalRelease::released_bytes),
        Ok(Some(ByteCount::new(4)))
    );
    assert_eq!(
        operation.plan_failed(DeliveryStatus::PossiblySent),
        Err(TransitionError::InvalidState)
    );
}
