//! Exhaustive position-control error categories across private owner layers.

use kafka_client_core::{
    AssignedConsumerMachineError, AssignedTopicPartition, PartitionIndex,
    RetireAssignmentErrorKind, TopicId,
};

use crate::{
    clock::ClockError,
    consumer::{
        assigned_event::AssignedConsumerEventStoreError,
        assigned_owner_model::AssignedConsumerOwnerError,
    },
};

use super::{
    AssignedConsumerControlInputError, control_error::control_error_kind,
    control_result::AssignedConsumerControlErrorKind, result::AssignedConsumerPortError,
    shard::AssignedConsumerShardLockError,
};

#[test]
fn private_position_rejections_translate_to_stable_categories() {
    let unknown = AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(7));
    let cases = [
        (
            AssignedConsumerPortError::Clock(ClockError::DeadlineOverflow),
            AssignedConsumerControlErrorKind::DeadlineOverflow,
        ),
        (
            AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::Contended),
            AssignedConsumerControlErrorKind::Contended,
        ),
        (
            owner(AssignedConsumerOwnerError::EffectsPending),
            AssignedConsumerControlErrorKind::Pending,
        ),
        (
            owner(AssignedConsumerOwnerError::Core(
                AssignedConsumerMachineError::NoAssignment,
            )),
            AssignedConsumerControlErrorKind::NoAssignment,
        ),
        (
            owner(AssignedConsumerOwnerError::Core(
                AssignedConsumerMachineError::UnknownPartition { partition: unknown },
            )),
            AssignedConsumerControlErrorKind::UnknownPartition,
        ),
        (
            owner(AssignedConsumerOwnerError::Core(
                AssignedConsumerMachineError::AssignmentRetirementRejected {
                    kind: RetireAssignmentErrorKind::ConsumerClosed,
                },
            )),
            AssignedConsumerControlErrorKind::InternalInvariant,
        ),
        (
            owner(AssignedConsumerOwnerError::ControlInput(
                AssignedConsumerControlInputError::NegativeOffset,
            )),
            AssignedConsumerControlErrorKind::NegativeOffset,
        ),
        (
            owner(AssignedConsumerOwnerError::Event(
                AssignedConsumerEventStoreError::Capacity,
            )),
            AssignedConsumerControlErrorKind::ResourceExhausted,
        ),
        (
            AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::OwnerMissing),
            AssignedConsumerControlErrorKind::HostUnavailable,
        ),
    ];

    for (private, expected) in cases {
        assert_eq!(control_error_kind(&private), expected);
    }
}

fn owner(error: AssignedConsumerOwnerError) -> AssignedConsumerPortError {
    AssignedConsumerPortError::Owner { error, wake: None }
}
