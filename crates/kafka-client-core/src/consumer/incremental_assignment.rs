//! Atomic direct-assignment additions and removals with stable survivor fences.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerMachineError,
    AssignedConsumerTransition, AssignedPartition, AssignedTopicPartition,
    machine::DirectAssignment, position::AssignedPartitionState,
};
use crate::{Deadline, Moment};

impl AssignedConsumerMachine {
    pub(super) fn add_assignments(
        &mut self,
        partitions: Vec<AssignedPartition>,
        now: Moment,
        resolution_deadline: Deadline,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        self.ensure_open()?;
        if partitions.is_empty() {
            return Ok(self.inert_assignment_change());
        }
        validate_additions(self.assignment.as_ref(), &partitions)?;
        let epoch = self.next_epoch;
        let next_epoch = epoch
            .checked_next()
            .ok_or(AssignedConsumerMachineError::AssignmentEpochExhausted)?;
        let mut states = Vec::new();
        let mut effects = Vec::new();
        if !reserve_assignment_change(&mut states, partitions.len())
            || !reserve_assignment_change(&mut effects, partitions.len())
        {
            return Err(AssignedConsumerMachineError::AssignmentChangeAllocationFailed);
        }
        for partition in partitions {
            let (state, effect) =
                AssignedPartitionState::new(epoch, partition, now, resolution_deadline)?;
            states.push(state);
            effects.push(effect);
        }
        match self.assignment.as_mut() {
            Some(assignment) => {
                if !reserve_assignment_change(&mut assignment.partitions, states.len()) {
                    return Err(AssignedConsumerMachineError::AssignmentChangeAllocationFailed);
                }
                assignment.partitions.extend(states);
                assignment.epoch = epoch;
            }
            None => {
                self.assignment = Some(DirectAssignment {
                    epoch,
                    partitions: states,
                });
            }
        }
        self.next_epoch = next_epoch;
        Ok(AssignedConsumerTransition::new(epoch, effects))
    }

    pub(super) fn remove_assignments(
        &mut self,
        partitions: &[AssignedTopicPartition],
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        self.ensure_open()?;
        if partitions.is_empty() {
            return Ok(self.inert_assignment_change());
        }
        validate_removals(self.assignment.as_ref(), partitions)?;
        let epoch = self.next_epoch;
        let next_epoch = epoch
            .checked_next()
            .ok_or(AssignedConsumerMachineError::AssignmentEpochExhausted)?;
        let assignment = self
            .assignment
            .as_mut()
            .unwrap_or_else(|| unreachable!("validated removals require an assignment"));
        let mut effects = Vec::new();
        if !reserve_assignment_change(&mut effects, partitions.len()) {
            return Err(AssignedConsumerMachineError::AssignmentChangeAllocationFailed);
        }
        effects.extend(partitions.iter().map(|partition| {
            let state = assignment
                .find(*partition)
                .unwrap_or_else(|| unreachable!("validated removal remains assigned"));
            AssignedConsumerEffect::Revoke {
                assignment_epoch: state.assignment_epoch(),
                partition: *partition,
            }
        }));
        assignment
            .partitions
            .retain(|state| !partitions.contains(&state.partition));
        assignment.epoch = epoch;
        self.next_epoch = next_epoch;
        Ok(AssignedConsumerTransition::new(epoch, effects))
    }

    fn inert_assignment_change(&self) -> AssignedConsumerTransition {
        match self.assignment_epoch() {
            Some(epoch) => AssignedConsumerTransition::new(epoch, Vec::new()),
            None => AssignedConsumerTransition::without_assignment(Vec::new()),
        }
    }
}

fn validate_additions(
    assignment: Option<&DirectAssignment>,
    partitions: &[AssignedPartition],
) -> Result<(), AssignedConsumerMachineError> {
    for (index, partition) in partitions.iter().enumerate() {
        if partitions[..index]
            .iter()
            .any(|present| present.partition() == partition.partition())
        {
            return Err(AssignedConsumerMachineError::DuplicatePartition {
                partition: partition.partition(),
            });
        }
    }
    if let Some(assignment) = assignment {
        for partition in partitions {
            if assignment.find(partition.partition()).is_some() {
                return Err(AssignedConsumerMachineError::PartitionAlreadyAssigned {
                    partition: partition.partition(),
                });
            }
        }
    }
    Ok(())
}

fn validate_removals(
    assignment: Option<&DirectAssignment>,
    partitions: &[AssignedTopicPartition],
) -> Result<(), AssignedConsumerMachineError> {
    validate_unique(partitions)?;
    let assignment = assignment.ok_or(AssignedConsumerMachineError::NoAssignment)?;
    for partition in partitions {
        if assignment.find(*partition).is_none() {
            return Err(AssignedConsumerMachineError::UnknownPartition {
                partition: *partition,
            });
        }
    }
    Ok(())
}

fn validate_unique(
    partitions: &[AssignedTopicPartition],
) -> Result<(), AssignedConsumerMachineError> {
    for (index, partition) in partitions.iter().enumerate() {
        if partitions[..index].contains(partition) {
            return Err(AssignedConsumerMachineError::DuplicatePartition {
                partition: *partition,
            });
        }
    }
    Ok(())
}

pub(super) fn reserve_assignment_change<T>(values: &mut Vec<T>, additional: usize) -> bool {
    values.try_reserve_exact(additional).is_ok()
}
