//! Stable public assignment naming derived from the authoritative group catalog.

use kafka_client_core::{GroupAssignmentPartition, LiveGroupAssignment};

use crate::consumer::{GroupConsumerAssignment, GroupConsumerAssignmentPartition};

use super::super::session_catalog::GroupSessionCatalog;

pub(super) fn named_assignment(
    catalog: &GroupSessionCatalog,
    assignment: &LiveGroupAssignment,
) -> GroupConsumerAssignment {
    named_assignment_partitions(catalog, assignment, assignment.partitions())
}

pub(super) fn named_assignment_subset(
    catalog: &GroupSessionCatalog,
    assignment: &LiveGroupAssignment,
    subset: &[GroupAssignmentPartition],
) -> GroupConsumerAssignment {
    debug_assert!(
        subset
            .iter()
            .all(|partition| assignment.partitions().binary_search(partition).is_ok())
    );
    named_assignment_partitions(catalog, assignment, subset)
}

fn named_assignment_partitions(
    catalog: &GroupSessionCatalog,
    assignment: &LiveGroupAssignment,
    assigned_partitions: &[GroupAssignmentPartition],
) -> GroupConsumerAssignment {
    let mut partitions = Vec::with_capacity(assigned_partitions.len());
    for assigned in assigned_partitions {
        let topic = catalog
            .topic_name(assigned.topic_id())
            .unwrap_or_else(|_error| unreachable!("installed assignment topics are cataloged"));
        let partition = i32::try_from(assigned.partition().get())
            .unwrap_or_else(|_error| unreachable!("installed partition fits Kafka i32"));
        partitions.push(GroupConsumerAssignmentPartition::new(
            topic.clone(),
            partition,
        ));
    }
    GroupConsumerAssignment::new(assignment.assignment_generation().get(), partitions)
}
