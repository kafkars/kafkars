//! Atomic transitions for one concrete directly assigned consumer.

use super::AssignedConsumerInput::FetchFailed;
use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, AssignedConsumerTransition, AssignedTopicPartition,
    AssignmentEpoch, RetireAssignment, RetireAssignmentErrorKind, StartPosition,
    machine::DirectAssignment, position::AssignedPartitionState,
};
use crate::{Deadline, Moment};

impl AssignedConsumerMachine {
    /// Applies one fact or control request without hidden effects.
    pub fn apply(
        &mut self,
        input: AssignedConsumerInput,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        match input {
            AssignedConsumerInput::BeginClose => self.begin_close(),
            AssignedConsumerInput::CloseDrained { close_id } => self.close_drained(close_id),
            AssignedConsumerInput::Assign {
                partitions,
                now,
                resolution_deadline,
            } => self.assign(partitions, now, resolution_deadline),
            AssignedConsumerInput::AddAssignments {
                partitions,
                now,
                resolution_deadline,
            } => self.add_assignments(partitions, now, resolution_deadline),
            AssignedConsumerInput::RemoveAssignments { partitions } => {
                self.remove_assignments(&partitions)
            }
            AssignedConsumerInput::RetireAssignment { assignment_epoch } => self
                .retire_assignment(RetireAssignment::new(assignment_epoch))
                .map_err(|error| reusable_retirement_error(error.kind())),
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
            } => self.seek(
                assignment_epoch,
                partition,
                position,
                now,
                resolution_deadline,
            ),
            AssignedConsumerInput::PositionResolved {
                fence,
                next_offset,
                now,
                throttle_ticks,
            } => {
                let (current, state) =
                    self.fenced_partition_mut(fence.assignment_epoch(), fence.partition())?;
                let effect = state.position_resolved(fence, next_offset, now, throttle_ticks)?;
                Ok(AssignedConsumerTransition::new(current, vec![effect]))
            }
            AssignedConsumerInput::PositionResolutionFailed {
                fence,
                now,
                failure,
            } => {
                let (current, state) =
                    self.fenced_partition_mut(fence.assignment_epoch(), fence.partition())?;
                let effect = state.position_resolution_failed(fence, now, failure)?;
                Ok(AssignedConsumerTransition::new(current, vec![effect]))
            }
            AssignedConsumerInput::PositionResolutionDeadlineElapsed { fence, now } => {
                let (current, state) =
                    self.fenced_partition_mut(fence.assignment_epoch(), fence.partition())?;
                let effect = state.position_resolution_deadline_elapsed(fence, now)?;
                Ok(AssignedConsumerTransition::new(current, vec![effect]))
            }
            AssignedConsumerInput::PositionThrottleElapsed { fence, now } => {
                let (current, state) =
                    self.fenced_partition_mut(fence.assignment_epoch(), fence.partition())?;
                let effect = state.position_throttle_elapsed(fence, now)?;
                Ok(AssignedConsumerTransition::new(current, vec![effect]))
            }
            AssignedConsumerInput::FetchAdvanced {
                fence,
                records,
                next_offset,
                now,
                throttle_ticks,
            } => self.fetch_advanced(fence, records, next_offset, now, throttle_ticks),
            FetchFailed { fence, failure } => self.fetch_failed(fence, failure),
            AssignedConsumerInput::FetchRetryAuthorized { fence } => self.fetch_retry(fence),
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
        self.ensure_open()?;
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
                    assignment_epoch: state.assignment_epoch(),
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
        self.ensure_open()?;
        let effect = self.assignment_mut(epoch)?.find_mut(partition)?.pause()?;
        Ok(AssignedConsumerTransition::new(
            epoch,
            effect.into_iter().collect(),
        ))
    }

    fn resume(
        &mut self,
        epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
        now: Moment,
        deadline: Deadline,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        self.ensure_open()?;
        let effect = self
            .assignment_mut(epoch)?
            .find_mut(partition)?
            .resume(now, deadline)?;
        Ok(AssignedConsumerTransition::new(
            epoch,
            effect.into_iter().collect(),
        ))
    }

    fn seek(
        &mut self,
        epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
        position: StartPosition,
        now: Moment,
        deadline: Deadline,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        self.ensure_open()?;
        let effects = self
            .assignment_mut(epoch)?
            .find_mut(partition)?
            .seek(position, now, deadline)?;
        Ok(AssignedConsumerTransition::new(epoch, effects))
    }

    pub(super) fn assignment_mut(
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

const fn reusable_retirement_error(
    kind: RetireAssignmentErrorKind,
) -> AssignedConsumerMachineError {
    match kind {
        RetireAssignmentErrorKind::ConsumerClosed => AssignedConsumerMachineError::ConsumerClosed,
        kind => AssignedConsumerMachineError::AssignmentRetirementRejected { kind },
    }
}
