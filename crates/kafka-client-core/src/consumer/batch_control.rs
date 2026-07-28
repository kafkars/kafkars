//! Atomic caller-ordered pause and retained-position resume transitions.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerMachineError,
    AssignedConsumerTransition, AssignedTopicPartition, AssignmentEpoch,
};
use crate::{Deadline, Moment};

impl AssignedConsumerMachine {
    /// Pauses one unique caller-ordered partition batch after complete preflight.
    pub fn pause_partitions(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        partitions: &[AssignedTopicPartition],
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        self.ensure_open()?;
        if partitions.is_empty() {
            return Ok(AssignedConsumerTransition::without_assignment(Vec::new()));
        }
        validate_unique(partitions)?;
        let assignment = self.assignment_for_batch(assignment_epoch)?;
        let mut plans = Vec::new();
        let mut effects = Vec::new();
        reserve_batch(&mut plans, partitions.len(), &mut effects)?;
        for partition in partitions {
            let index = assignment
                .partitions
                .iter()
                .position(|state| state.partition == *partition)
                .ok_or(AssignedConsumerMachineError::UnknownPartition {
                    partition: *partition,
                })?;
            plans.push((index, assignment.partitions[index].plan_pause()?));
        }
        let assignment = self
            .assignment
            .as_mut()
            .unwrap_or_else(|| unreachable!("batch preflight retained the assignment"));
        for (index, plan) in plans {
            if let Some(effect) =
                assignment.partitions[index].install_planned_pause(assignment_epoch, plan)
            {
                effects.push(effect);
            }
        }
        Ok(AssignedConsumerTransition::new(assignment_epoch, effects))
    }

    /// Resumes one unique caller-ordered batch only from group-retained positions.
    pub fn resume_retained_partitions(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        partitions: &[AssignedTopicPartition],
        now: Moment,
        resolution_deadline: Deadline,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        self.ensure_open()?;
        if partitions.is_empty() {
            return Ok(AssignedConsumerTransition::without_assignment(Vec::new()));
        }
        validate_unique(partitions)?;
        let assignment = self.assignment_for_batch(assignment_epoch)?;
        let mut plans = Vec::new();
        let mut effects = Vec::new();
        reserve_batch(&mut plans, partitions.len(), &mut effects)?;
        for partition in partitions {
            let index = assignment
                .partitions
                .iter()
                .position(|state| state.partition == *partition)
                .ok_or(AssignedConsumerMachineError::UnknownPartition {
                    partition: *partition,
                })?;
            plans.push((
                index,
                assignment.partitions[index].plan_retained_resume(
                    assignment_epoch,
                    now,
                    resolution_deadline,
                )?,
            ));
        }
        let assignment = self
            .assignment
            .as_mut()
            .unwrap_or_else(|| unreachable!("batch preflight retained the assignment"));
        for (index, plan) in plans {
            if let Some(effect) = assignment.partitions[index].install_planned_resume(plan) {
                effects.push(effect);
            }
        }
        Ok(AssignedConsumerTransition::new(assignment_epoch, effects))
    }

    fn assignment_for_batch(
        &self,
        supplied: AssignmentEpoch,
    ) -> Result<&super::machine::DirectAssignment, AssignedConsumerMachineError> {
        let assignment = self
            .assignment
            .as_ref()
            .ok_or(AssignedConsumerMachineError::NoAssignment)?;
        if assignment.epoch != supplied {
            return Err(AssignedConsumerMachineError::StaleAssignment {
                active: assignment.epoch,
                supplied,
            });
        }
        Ok(assignment)
    }
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

fn reserve_batch<T>(
    plans: &mut Vec<(usize, T)>,
    count: usize,
    effects: &mut Vec<AssignedConsumerEffect>,
) -> Result<(), AssignedConsumerMachineError> {
    plans
        .try_reserve_exact(count)
        .and_then(|()| effects.try_reserve_exact(count))
        .map_err(|_error| AssignedConsumerMachineError::ControlAllocationFailed)
}
