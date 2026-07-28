//! Bounded top-level reassignment response scenarios.

use kafka_wire::AlterPartitionReassignmentsResponse;

use super::{
    AlterPartitionReassignmentRef, ValidatedAlterPartitionReassignmentsResponse,
    validate_alter_partition_reassignments_response,
};

#[test]
fn top_level_error_stays_separate_and_v1_policy_mismatch_is_invalid() {
    let changes = [AlterPartitionReassignmentRef::new("orders", 0, Some(&[1]))];
    let mut response = AlterPartitionReassignmentsResponse::default();
    response.error_code = -31_000;
    response.error_message = Some("top".into());
    let validated =
        validate_alter_partition_reassignments_response(&changes, true, &response, 1, usize::MAX)
            .unwrap_or_else(|error| panic!("top-level response: {error:?}"));
    let ValidatedAlterPartitionReassignmentsResponse::BrokerRejected(error) = validated else {
        panic!("top-level rejection expected");
    };
    assert_eq!(error.code(), -31_000);
    assert_eq!(error.message(), Some("top"));

    response.allow_replication_factor_change = false;
    assert!(
        validate_alter_partition_reassignments_response(&changes, true, &response, 1, usize::MAX,)
            .is_err()
    );
}

#[test]
fn explicit_false_accepts_v1_echo_and_rejects_v0_or_true_echo() {
    let changes = [AlterPartitionReassignmentRef::new(
        "orders",
        0,
        Some(&[1, 2]),
    )];
    let mut partition =
        kafka_wire::alter_partition_reassignments_response::ReassignablePartitionResponse::default(
        );
    partition.partition_index = 0;
    let mut topic =
        kafka_wire::alter_partition_reassignments_response::ReassignableTopicResponse::default();
    topic.name = "orders".into();
    topic.partitions = vec![partition];
    let mut response = AlterPartitionReassignmentsResponse::default();
    response.allow_replication_factor_change = false;
    response.responses = vec![topic];

    assert!(
        validate_alter_partition_reassignments_response(&changes, false, &response, 1, usize::MAX,)
            .is_ok()
    );
    assert!(
        validate_alter_partition_reassignments_response(&changes, false, &response, 0, usize::MAX,)
            .is_err()
    );

    response.allow_replication_factor_change = true;
    assert!(
        validate_alter_partition_reassignments_response(&changes, false, &response, 1, usize::MAX,)
            .is_err()
    );
}
