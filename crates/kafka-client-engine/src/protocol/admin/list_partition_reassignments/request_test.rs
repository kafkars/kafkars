//! Request-shape tests for explicit and all-active reassignment selections.

use kafka_client_core::{ListPartitionReassignmentTarget, ListPartitionReassignmentsPlan};

use super::list_partition_reassignments_request;

#[test]
fn selected_request_groups_topics_without_losing_partition_order() {
    let plan = ListPartitionReassignmentsPlan::selected(vec![
        ListPartitionReassignmentTarget::new("z".to_owned(), 2),
        ListPartitionReassignmentTarget::new("a".to_owned(), 0),
        ListPartitionReassignmentTarget::new("z".to_owned(), 1),
    ])
    .unwrap_or_else(|error| panic!("valid selection: {error}"));
    let request = list_partition_reassignments_request(&plan, 73);
    assert_eq!(request.timeout_ms, 73);
    let topics = request
        .topics
        .unwrap_or_else(|| panic!("expected selected topics"));
    assert_eq!(topics.len(), 2);
    assert_eq!(topics[0].name.as_str(), "z");
    assert_eq!(topics[0].partition_indexes, vec![2, 1]);
    assert_eq!(topics[1].name.as_str(), "a");
    assert_eq!(topics[1].partition_indexes, vec![0]);
}

#[test]
fn all_active_is_nullable_instead_of_an_empty_array() {
    let request =
        list_partition_reassignments_request(&ListPartitionReassignmentsPlan::all_active(), 5);
    assert_eq!(request.timeout_ms, 5);
    assert_eq!(request.topics, None);
}
