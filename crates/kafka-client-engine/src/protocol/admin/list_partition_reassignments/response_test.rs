//! Response normalization tests for controller reassignment queries.

use kafka_client_core::{
    ListPartitionReassignmentTarget, ListPartitionReassignmentsInput,
    ListPartitionReassignmentsPlan,
};
use kafka_wire::{
    ListPartitionReassignmentsResponse,
    list_partition_reassignments_response::{
        OngoingPartitionReassignment, OngoingTopicReassignment,
    },
};

use super::{
    ListPartitionReassignmentsProtocolFailure, normalize_list_partition_reassignments_response,
};

type PartitionReassignment<'a> = (i32, &'a [i32], &'a [i32], &'a [i32]);

fn topic(name: &str, partitions: &[PartitionReassignment<'_>]) -> OngoingTopicReassignment {
    let mut topic = OngoingTopicReassignment::default();
    topic.name = name.into();
    topic.partitions = partitions
        .iter()
        .map(|(index, replicas, adding, removing)| {
            let mut partition = OngoingPartitionReassignment::default();
            partition.partition_index = *index;
            partition.replicas = replicas.to_vec();
            partition.adding_replicas = adding.to_vec();
            partition.removing_replicas = removing.to_vec();
            partition
        })
        .collect();
    topic
}

#[test]
fn selected_rows_are_restored_to_caller_order_and_inactive_targets_are_absent() {
    let plan = ListPartitionReassignmentsPlan::selected(vec![
        ListPartitionReassignmentTarget::new("z".to_owned(), 2),
        ListPartitionReassignmentTarget::new("missing".to_owned(), 0),
        ListPartitionReassignmentTarget::new("a".to_owned(), 0),
    ])
    .unwrap_or_else(|error| panic!("valid selection: {error}"));
    let mut response = ListPartitionReassignmentsResponse::default();
    response.throttle_time_ms = 9;
    response.error_message = None;
    response.topics = vec![
        topic("a", &[(0, &[1, 2], &[2], &[])]),
        topic("z", &[(2, &[4], &[], &[3])]),
    ];
    let input = normalize_list_partition_reassignments_response(&plan, &response, 0, 1 << 20)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let ListPartitionReassignmentsInput::BrokerResponded { batch } = input else {
        panic!("expected response batch");
    };
    assert_eq!(batch.throttle_time_ms(), 9);
    assert_eq!(batch.reassignments().len(), 2);
    assert_eq!(batch.reassignments()[0].topic(), "z");
    assert_eq!(batch.reassignments()[1].topic(), "a");
}

#[test]
fn all_active_rows_use_topic_byte_and_partition_order() {
    let mut response = ListPartitionReassignmentsResponse::default();
    response.error_message = None;
    response.topics = vec![
        topic("z", &[(2, &[1], &[], &[]), (0, &[2], &[], &[])]),
        topic("a", &[(1, &[3], &[], &[])]),
    ];
    let input = normalize_list_partition_reassignments_response(
        &ListPartitionReassignmentsPlan::all_active(),
        &response,
        0,
        1 << 20,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let ListPartitionReassignmentsInput::BrokerResponded { batch } = input else {
        panic!("expected response batch");
    };
    let identities: Vec<_> = batch
        .reassignments()
        .iter()
        .map(|outcome| (outcome.topic(), outcome.partition()))
        .collect();
    assert_eq!(identities, vec![("a", 1), ("z", 0), ("z", 2)]);
}

#[test]
fn top_level_error_preserves_signed_code_and_utf8_safe_truncation() {
    let mut response = ListPartitionReassignmentsResponse::default();
    response.error_code = -41;
    response.error_message = Some(format!("{}é", "x".repeat(1023)).as_str().into());
    let input = normalize_list_partition_reassignments_response(
        &ListPartitionReassignmentsPlan::all_active(),
        &response,
        0,
        1 << 20,
    )
    .unwrap_or_else(|error| panic!("valid broker terminal: {error:?}"));
    let ListPartitionReassignmentsInput::BrokerRejected { error } = input else {
        panic!("expected broker error");
    };
    assert_eq!(error.code(), -41);
    assert_eq!(
        error
            .message()
            .unwrap_or_else(|| panic!("expected message"))
            .len(),
        1023
    );
    assert!(error.message_truncated());
}

#[test]
fn hostile_shapes_are_rejected_before_normalized_allocation() {
    let mut response = ListPartitionReassignmentsResponse::default();
    response.error_message = None;
    response.topics = vec![topic("a", &[(0, &[1, 1], &[], &[])])];
    assert_eq!(
        normalize_list_partition_reassignments_response(
            &ListPartitionReassignmentsPlan::all_active(),
            &response,
            0,
            1 << 20,
        ),
        Err(ListPartitionReassignmentsProtocolFailure::DuplicateBrokerId { actual: 1 })
    );
}
