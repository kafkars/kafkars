//! Stable public assignment naming derived from the authoritative group catalog.

use kafka_client_core::LiveGroupAssignment;

use crate::consumer::{GroupConsumerAssignment, GroupConsumerAssignmentPartition};

use super::super::session_catalog::GroupSessionCatalog;

pub(super) fn named_assignment(
    catalog: &GroupSessionCatalog,
    assignment: &LiveGroupAssignment,
) -> GroupConsumerAssignment {
    let mut partitions = Vec::with_capacity(assignment.partitions().len());
    for assigned in assignment.partitions() {
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
