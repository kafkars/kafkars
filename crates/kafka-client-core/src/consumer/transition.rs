//! Atomic transitions for one concrete directly assigned consumer.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, AssignedConsumerTransition, AssignedTopicPartition,
    AssignmentEpoch, machine::DirectAssignment, position::AssignedPartitionState,
};
use crate::{Deadline, Moment};

impl AssignedConsumerMachine {
    /// Applies one fact or control request without hidden effects.
    pub fn apply(
        &mut self,
        input: AssignedConsumerInput,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        match input {
            AssignedConsumerInput::Assign {
                partitions,
                now,
                resolution_deadline,
            } => self.assign(partitions, now, resolution_deadline),
            AssignedConsumerInput::Pause {
                assignment_epoch,
                partition,
            } => self.pause(assignment_epoch, partition),
            AssignedConsumerInput::Resume {
                assignment_epoch,
                partition,
                now,
                resolution_deadline,
            } => self.resume(assignment_epoch, partition, now, resolution_deadline),
            AssignedConsumerInput::Seek {
                assignment_epoch,
                partition,
                position,
                now,
                resolution_deadline,
            } => {
                let assignment = self.assignment_mut(assignment_epoch)?;
                let effects = assignment.find_mut(partition)?.seek(
                    assignment_epoch,
                    position,
                    now,
                    resolution_deadline,
                )?;
                Ok(AssignedConsumerTransition::new(assignment_epoch, effects))
            }
            AssignedConsumerInput::PositionResolved {
                fence,
                next_offset,
                now,
                throttle_ticks,
            } => {
                let assignment = self.assignment_mut(fence.assignment_epoch())?;
                let effect = assignment.find_mut(fence.partition())?.position_resolved(
                    fence,
                    next_offset,
                    now,
                    throttle_ticks,
                )?;
                Ok(AssignedConsumerTransition::new(
                    fence.assignment_epoch(),
                    vec![effect],
                ))
            }
            AssignedConsumerInput::PositionResolutionFailed { fence, now } => {
                let assignment = self.assignment_mut(fence.assignment_epoch())?;
                let effect = assignment
                    .find_mut(fence.partition())?
                    .position_resolution_failed(fence, now)?;
                Ok(AssignedConsumerTransition::new(
                    fence.assignment_epoch(),
                    vec![effect],
                ))
            }
            AssignedConsumerInput::PositionResolutionDeadlineElapsed { fence, now } => {
                let assignment = self.assignment_mut(fence.assignment_epoch())?;
                let effect = assignment
                    .find_mut(fence.partition())?
                    .position_resolution_deadline_elapsed(fence, now)?;
                Ok(AssignedConsumerTransition::new(
                    fence.assignment_epoch(),
                    vec![effect],
                ))
            }
            AssignedConsumerInput::PositionThrottleElapsed { fence, now } => {
                let assignment = self.assignment_mut(fence.assignment_epoch())?;
                let effect = assignment
                    .find_mut(fence.partition())?
                    .position_throttle_elapsed(fence, now)?;
                Ok(AssignedConsumerTransition::new(
                    fence.assignment_epoch(),
                    vec![effect],
                ))
            }
            AssignedConsumerInput::FetchAdvanced {
                fence,
                next_offset,
                now,
                throttle_ticks,
            } => self.fetch_advanced(fence, next_offset, now, throttle_ticks),
            AssignedConsumerInput::FetchThrottleElapsed { fence, now } => {
                self.fetch_throttle_elapsed(fence, now)
            }
        }
    }

    fn assign(
        &mut self,
        partitions: Vec<super::AssignedPartition>,
        now: Moment,
        deadline: Deadline,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        DirectAssignment::validate(&partitions)?;
        let epoch = self.next_epoch;
        let next_epoch = epoch
            .checked_next()
            .ok_or(AssignedConsumerMachineError::AssignmentEpochExhausted)?;
        let mut states = Vec::with_capacity(partitions.len());
        let mut start_effects = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let (state, effect) = AssignedPartitionState::new(epoch, partition, now, deadline)?;
            states.push(state);
            start_effects.push(effect);
        }
        let mut effects = Vec::with_capacity(
            self.assignment
                .as_ref()
                .map_or(0, |assignment| assignment.partitions.len())
                + start_effects.len(),
        );
        if let Some(assignment) = &self.assignment {
            effects.extend(assignment.partitions.iter().map(|state| {
                AssignedConsumerEffect::Revoke {
                    assignment_epoch: assignment.epoch,
                    partition: state.partition,
                }
            }));
        }
        effects.extend(start_effects);
        self.assignment = Some(DirectAssignment {
            epoch,
            partitions: states,
        });
        self.next_epoch = next_epoch;
        Ok(AssignedConsumerTransition::new(epoch, effects))
    }

    fn pause(
        &mut self,
        epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        let effect = self
            .assignment_mut(epoch)?
            .find_mut(partition)?
            .pause(epoch)?;
        Ok(AssignedConsumerTransition::new(
            epoch,
            effect.into_iter().collect(),
        ))
    }

    fn fetch_advanced(
        &mut self,
        fence: super::FetchFence,
        next_offset: super::NextFetchOffset,
        now: Moment,
        throttle_ticks: u64,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        let position = fence.position();
        let effect = self
            .assignment_mut(position.assignment_epoch())?
            .find_mut(position.partition())?
            .fetch_advanced(fence, next_offset, now, throttle_ticks)?;
        Ok(AssignedConsumerTransition::new(
            position.assignment_epoch(),
            vec![effect],
        ))
    }

    fn fetch_throttle_elapsed(
        &mut self,
        fence: super::FetchFence,
        now: Moment,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        let position = fence.position();
        let effect = self
            .assignment_mut(position.assignment_epoch())?
            .find_mut(position.partition())?
            .fetch_throttle_elapsed(fence, now)?;
        Ok(AssignedConsumerTransition::new(
            position.assignment_epoch(),
            vec![effect],
        ))
    }

    fn resume(
        &mut self,
        epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
        now: Moment,
        deadline: Deadline,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        let effect = self
            .assignment_mut(epoch)?
            .find_mut(partition)?
            .resume(epoch, now, deadline)?;
        Ok(AssignedConsumerTransition::new(
            epoch,
            effect.into_iter().collect(),
        ))
    }

    fn assignment_mut(
        &mut self,
        supplied: AssignmentEpoch,
    ) -> Result<&mut DirectAssignment, AssignedConsumerMachineError> {
        let assignment = self
            .assignment
            .as_mut()
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
