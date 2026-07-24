//! Unique owner of one direct assignment and its partition positions.

use super::{
    AssignedConsumerMachineError, AssignedPartition, AssignedTopicPartition, AssignmentEpoch,
    close::AssignedConsumerCloseState, position::AssignedPartitionState,
};

/// Deterministic owner of direct-assignment epochs and fetch positions.
#[derive(Debug)]
pub struct AssignedConsumerMachine {
    pub(super) next_epoch: AssignmentEpoch,
    pub(super) assignment: Option<DirectAssignment>,
    pub(super) close_state: AssignedConsumerCloseState,
}

impl AssignedConsumerMachine {
    /// Creates an unassigned direct consumer.
    pub const fn new() -> Self {
        Self {
            next_epoch: AssignmentEpoch::initial(),
            assignment: None,
            close_state: AssignedConsumerCloseState::Open,
        }
    }

    /// Returns the retained assignment epoch, when one has been installed.
    ///
    /// A closed machine retains this identity only to fence draining work.
    pub const fn assignment_epoch(&self) -> Option<AssignmentEpoch> {
        match &self.assignment {
            Some(assignment) => Some(assignment.epoch),
            None => None,
        }
    }

    /// Reports whether direct-consumer work admission is permanently closed.
    pub const fn is_closed(&self) -> bool {
        self.close_state.is_closed()
    }

    pub(super) const fn ensure_open(&self) -> Result<(), AssignedConsumerMachineError> {
        if self.close_state.is_closed() {
            Err(AssignedConsumerMachineError::ConsumerClosed)
        } else {
            Ok(())
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
