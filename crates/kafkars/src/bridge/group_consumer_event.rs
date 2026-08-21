//! Private translation of current engine-owned classic-group state.

use kafka_client_engine::{
    GroupConsumerAssignment as EngineAssignment, GroupConsumerMetadata as EngineMetadata,
    GroupConsumerState as EngineState, GroupConsumerStateError, GroupConsumerStateErrorKind,
};

use super::group_consumer_metadata::GroupConsumerMetadata as BridgeMetadata;
use crate::consumer::{ConsumerAssignment, ConsumerAssignmentPartition, GroupMetadata};
use crate::{ErrorKind, KafkaError};

pub(super) fn translate_assignment(assignment: &EngineAssignment) -> ConsumerAssignment {
    let partitions = assignment
        .partitions()
        .iter()
        .map(|partition| {
            ConsumerAssignmentPartition::from_parts(
                partition.topic().to_owned(),
                partition.partition(),
            )
        })
        .collect();
    ConsumerAssignment::from_parts(assignment.assignment_epoch(), partitions)
}

pub(super) fn translate_group_consumer_state(
    state: EngineState,
) -> (ConsumerAssignment, GroupMetadata) {
    let (assignment, metadata) = state.into_parts();
    (
        translate_assignment(&assignment),
        translate_metadata(metadata),
    )
}

fn translate_metadata(metadata: EngineMetadata) -> GroupMetadata {
    GroupMetadata::from_bridge(BridgeMetadata::from_engine(metadata))
}

pub(super) fn translate_group_consumer_state_error(error: GroupConsumerStateError) -> KafkaError {
    let (kind, message) = match error.kind() {
        GroupConsumerStateErrorKind::Contended => (
            ErrorKind::Backpressure,
            "group state observation is contended",
        ),
        GroupConsumerStateErrorKind::HostUnavailable => {
            (ErrorKind::Internal, "group state owner is unavailable")
        }
        GroupConsumerStateErrorKind::Allocation => (
            ErrorKind::Backpressure,
            "group state snapshot storage is unavailable",
        ),
        GroupConsumerStateErrorKind::InternalInvariant => {
            (ErrorKind::Internal, "group state ownership is inconsistent")
        }
    };
    KafkaError::new(kind, message)
}
