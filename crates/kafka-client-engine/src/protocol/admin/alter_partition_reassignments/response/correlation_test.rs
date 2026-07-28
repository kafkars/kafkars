//! Caller-order restoration for exact reassignment response identities.

use kafka_client_core::AlterPartitionReassignmentResult;
use kafka_wire::{
    AlterPartitionReassignmentsResponse,
    alter_partition_reassignments_response::{
        ReassignablePartitionResponse, ReassignableTopicResponse,
    },
};

use super::{
    AlterPartitionReassignmentRef, ValidatedAlterPartitionReassignmentsResponse,
    validate_alter_partition_reassignments_response,
};

#[test]
fn shuffled_response_restores_caller_order_and_exact_signed_error() {
    let changes = [
        AlterPartitionReassignmentRef::new("orders", 2, Some(&[1, 2])),
        AlterPartitionReassignmentRef::new("audit", 0, None),
    ];
    let mut rejected = ReassignablePartitionResponse::default();
    rejected.partition_index = 2;
    rejected.error_code = -32_000;
    rejected.error_message = Some("controller diagnostic".into());
    let mut orders = ReassignableTopicResponse::default();
    orders.name = "orders".into();
    orders.partitions = vec![rejected];
    let mut accepted = ReassignablePartitionResponse::default();
    accepted.partition_index = 0;
    accepted.error_code = 0;
    let mut audit = ReassignableTopicResponse::default();
    audit.name = "audit".into();
    audit.partitions = vec![accepted];
    let mut response = AlterPartitionReassignmentsResponse::default();
    response.throttle_time_ms = 7;
    response.responses = vec![audit, orders];

    let validated =
        validate_alter_partition_reassignments_response(&changes, true, &response, 1, usize::MAX)
            .unwrap_or_else(|error| panic!("response: {error:?}"));
    let ValidatedAlterPartitionReassignmentsResponse::Batch(batch) = validated else {
        panic!("batch expected");
    };
    assert_eq!(batch.throttle_time_ms(), 7);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    let AlterPartitionReassignmentResult::Failed(error) = batch.outcomes()[0].result() else {
        panic!("partition rejection expected");
    };
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.message(), Some("controller diagnostic"));
    assert_eq!(batch.outcomes()[1].topic(), "audit");
}
