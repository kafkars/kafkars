//! Exhaustive assignment-error category scenarios across private owner layers.

use kafka_client_core::{
    AssignedConsumerMachineError, AssignedTopicPartition, PartitionIndex, TopicId,
};

use crate::{
    clock::ClockError,
    consumer::{
        assigned_event::AssignedConsumerEventStoreError,
        assigned_owner_model::AssignedConsumerOwnerError, assigned_topics::AssignedTopicsError,
    },
};

use super::{
    assignment_error::assignment_error_kind,
    assignment_result::AssignedConsumerTryReplaceAssignmentErrorKind,
    result::AssignedConsumerPortError, shard::AssignedConsumerShardLockError,
};

#[test]
fn private_assignment_rejections_translate_to_stable_categories() {
    let duplicate = AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0));
    let cases = [
        (
            AssignedConsumerPortError::Clock(ClockError::InstantOverflow),
            AssignedConsumerTryReplaceAssignmentErrorKind::DeadlineOverflow,
        ),
        (
            AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::Contended),
            AssignedConsumerTryReplaceAssignmentErrorKind::Contended,
        ),
        (
            owner(AssignedConsumerOwnerError::EffectsPending),
            AssignedConsumerTryReplaceAssignmentErrorKind::Pending,
        ),
        (
            owner(AssignedConsumerOwnerError::Topics(
                AssignedTopicsError::PartitionCapacity {
                    actual: 3,
                    limit: 2,
                },
            )),
            AssignedConsumerTryReplaceAssignmentErrorKind::AssignmentCapacity,
        ),
        (
            owner(AssignedConsumerOwnerError::Core(
                AssignedConsumerMachineError::DuplicatePartition {
                    partition: duplicate,
                },
            )),
            AssignedConsumerTryReplaceAssignmentErrorKind::DuplicatePartition,
        ),
        (
            owner(AssignedConsumerOwnerError::Event(
                AssignedConsumerEventStoreError::Capacity,
            )),
            AssignedConsumerTryReplaceAssignmentErrorKind::EventCapacity,
        ),
        (
            AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::OwnerMissing),
            AssignedConsumerTryReplaceAssignmentErrorKind::HostUnavailable,
        ),
    ];

    for (private, expected) in cases {
        assert_eq!(assignment_error_kind(&private), expected);
    }
}

fn owner(error: AssignedConsumerOwnerError) -> AssignedConsumerPortError {
    AssignedConsumerPortError::Owner { error, wake: None }
}
