//! Caller-order restoration for validated reassignment-listing rows.

use core::cmp::Ordering;

use kafka_client_core::{
    ListPartitionReassignmentsSelection, PartitionReassignment, PartitionReassignmentOutcome,
};
use kafka_wire::{
    ListPartitionReassignmentsResponse,
    list_partition_reassignments_response::OngoingPartitionReassignment,
};

pub(super) fn normalize_rows(
    selection: &ListPartitionReassignmentsSelection,
    response: &ListPartitionReassignmentsResponse,
) -> Vec<PartitionReassignmentOutcome> {
    match selection {
        ListPartitionReassignmentsSelection::Selected(targets) => targets
            .iter()
            .filter_map(|target| {
                response
                    .topics
                    .iter()
                    .find(|topic| topic.name.as_str() == target.topic())
                    .and_then(|topic| {
                        topic
                            .partitions
                            .iter()
                            .find(|partition| partition.partition_index == target.partition())
                    })
                    .map(|partition| normalized_outcome(target.topic(), partition))
            })
            .collect(),
        ListPartitionReassignmentsSelection::AllActive => {
            let mut rows: Vec<_> = response
                .topics
                .iter()
                .flat_map(|topic| {
                    topic
                        .partitions
                        .iter()
                        .map(|partition| normalized_outcome(topic.name.as_str(), partition))
                })
                .collect();
            rows.sort_unstable_by(|left, right| {
                left.topic()
                    .as_bytes()
                    .cmp(right.topic().as_bytes())
                    .then_with(|| left.partition().cmp(&right.partition()))
            });
            debug_assert!(rows.windows(2).all(|pair| {
                match pair[0].topic().as_bytes().cmp(pair[1].topic().as_bytes()) {
                    Ordering::Less => true,
                    Ordering::Equal => pair[0].partition() < pair[1].partition(),
                    Ordering::Greater => false,
                }
            }));
            rows
        }
    }
}

fn normalized_outcome(
    topic: &str,
    partition: &OngoingPartitionReassignment,
) -> PartitionReassignmentOutcome {
    PartitionReassignmentOutcome::new(
        topic.to_owned(),
        partition.partition_index,
        PartitionReassignment::new(
            partition.replicas.clone(),
            partition.adding_replicas.clone(),
            partition.removing_replicas.clone(),
        ),
    )
}
