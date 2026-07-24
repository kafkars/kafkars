//! Unique owner of one direct assignment and its partition positions.

use super::{
    AssignedConsumerMachineError, AssignedPartition, AssignedTopicPartition, AssignmentEpoch,
    position::AssignedPartitionState,
};

/// Deterministic owner of direct-assignment epochs and fetch positions.
#[derive(Debug)]
pub struct AssignedConsumerMachine {
    pub(super) next_epoch: AssignmentEpoch,
    pub(super) assignment: Option<DirectAssignment>,
}

impl AssignedConsumerMachine {
    /// Creates an unassigned direct consumer.
    pub const fn new() -> Self {
        Self {
            next_epoch: AssignmentEpoch::initial(),
            assignment: None,
        }
    }

    /// Returns the active assignment epoch, when assigned.
    pub const fn assignment_epoch(&self) -> Option<AssignmentEpoch> {
        match &self.assignment {
            Some(assignment) => Some(assignment.epoch),
            None => None,
        }
    }
}

impl Default for AssignedConsumerMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(super) struct DirectAssignment {
    pub(super) epoch: AssignmentEpoch,
    pub(super) partitions: Vec<AssignedPartitionState>,
}

impl DirectAssignment {
    pub(super) fn validate(
        partitions: &[AssignedPartition],
    ) -> Result<(), AssignedConsumerMachineError> {
        if partitions.is_empty() {
            return Err(AssignedConsumerMachineError::EmptyAssignment);
        }
        for (index, candidate) in partitions.iter().enumerate() {
            if partitions[..index]
                .iter()
                .any(|present| present.partition() == candidate.partition())
            {
                return Err(AssignedConsumerMachineError::DuplicatePartition {
                    partition: candidate.partition(),
                });
            }
        }
        Ok(())
    }

    pub(super) fn find_mut(
        &mut self,
        partition: AssignedTopicPartition,
    ) -> Result<&mut AssignedPartitionState, AssignedConsumerMachineError> {
        self.partitions
            .iter_mut()
            .find(|state| state.partition == partition)
            .ok_or(AssignedConsumerMachineError::UnknownPartition { partition })
    }
}
