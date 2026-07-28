//! Exact caller-order restoration scenarios for reassignment rows.

use kafka_client_core::{ListPartitionReassignmentTarget, ListPartitionReassignmentsPlan};
use kafka_wire::{
    ListPartitionReassignmentsResponse,
    list_partition_reassignments_response::{
        OngoingPartitionReassignment, OngoingTopicReassignment,
    },
};

use super::correlation::normalize_rows;

fn topic(name: &str, partition: i32) -> OngoingTopicReassignment {
    let mut row = OngoingPartitionReassignment::default();
    row.partition_index = partition;
    row.replicas = vec![1];
    let mut topic = OngoingTopicReassignment::default();
    topic.name = name.into();
    topic.partitions = vec![row];
    topic
}

#[test]
fn selected_rows_follow_caller_order_and_omit_inactive_targets() {
    let plan = ListPartitionReassignmentsPlan::selected(vec![
        ListPartitionReassignmentTarget::new("z".to_owned(), 2),
        ListPartitionReassignmentTarget::new("missing".to_owned(), 0),
        ListPartitionReassignmentTarget::new("a".to_owned(), 0),
    ])
    .unwrap_or_else(|error| panic!("valid selected plan: {error}"));
    let mut response = ListPartitionReassignmentsResponse::default();
    response.topics = vec![topic("a", 0), topic("z", 2)];
    let rows = normalize_rows(plan.selection(), &response);
    let identities: Vec<_> = rows
        .iter()
        .map(|row| (row.topic(), row.partition()))
        .collect();
    assert_eq!(identities, vec![("z", 2), ("a", 0)]);
}
