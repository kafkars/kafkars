//! Exhaustive incremental-assignment error-category scenarios.

use kafka_client_core::{
    AssignedConsumerMachineError, AssignedTopicPartition, PartitionIndex, TopicId,
};

use super::{
    assignment_change_error::assignment_change_error_kind,
    assignment_change_result::AssignedConsumerTryChangeAssignmentErrorKind as Kind,
    result::AssignedConsumerPortError,
};
use crate::consumer::{
    assigned_owner_model::AssignedConsumerOwnerError, assigned_topics::AssignedTopicsError,
};

#[test]
fn semantic_change_rejections_remain_distinct() {
    let partition = AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0));
    let cases = [
        (
            owner(AssignedConsumerOwnerError::Core(
                AssignedConsumerMachineError::NoAssignment,
            )),
            Kind::NoAssignment,
        ),
        (
            owner(AssignedConsumerOwnerError::Core(
                AssignedConsumerMachineError::PartitionAlreadyAssigned { partition },
            )),
            Kind::AlreadyAssigned,
        ),
        (
            owner(AssignedConsumerOwnerError::Core(
                AssignedConsumerMachineError::UnknownPartition { partition },
            )),
            Kind::UnknownPartition,
        ),
        (
            owner(AssignedConsumerOwnerError::Topics(
                AssignedTopicsError::UnknownTopicName,
            )),
            Kind::UnknownPartition,
        ),
        (
            owner(AssignedConsumerOwnerError::Core(
                AssignedConsumerMachineError::AssignmentChangeAllocationFailed,
            )),
            Kind::ResourceExhausted,
        ),
    ];

    for (private, expected) in cases {
        assert_eq!(assignment_change_error_kind(&private), expected);
    }
}

fn owner(error: AssignedConsumerOwnerError) -> AssignedConsumerPortError {
    AssignedConsumerPortError::Owner { error, wake: None }
}
