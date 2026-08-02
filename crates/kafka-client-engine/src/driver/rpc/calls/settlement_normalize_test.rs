//! Conservative per-entry terminal normalization for malformed aggregate responses.

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, DeliveryStatus, Moment,
    ProducerAttemptFailureKind, ProducerInput,
};
use kafka_wire::ProduceResponse;

use super::{
    super::produce_call_entries::TrackedProduceEntry, settlement_normalize::normalized_entry_input,
};

#[test]
fn invalid_aggregate_shape_terminalizes_one_entry_as_possibly_sent() {
    let execution =
        BatchExecutionId::new(BatchId::from_raw(7), BatchExecutionGeneration::initial());
    let entry = TrackedProduceEntry {
        execution,
        deadline: Deadline::from_tick(20),
        topic: "orders".into(),
        partition: 3,
    };
    let now = Moment::from_tick(11);

    assert_eq!(
        normalized_entry_input(
            &entry,
            now,
            &Ok(ProduceResponse::default()),
            true,
            false,
            None,
        ),
        ProducerInput::TransportFailed {
            execution,
            now,
            failure: ProducerAttemptFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
            route_refreshed: false,
        }
    );
}
