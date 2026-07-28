//! Deterministic transitions for sequential group-position reset.

use crate::{Moment, NextFetchOffset, PositionResolutionAttemptFailure};

use super::super::{GroupPositionBatch, GroupPositionPartitionFact, GroupPositionPartitionResult};
use super::{
    GroupPositionResetApplyError, GroupPositionResetEffect, GroupPositionResetFailure,
    GroupPositionResetInput, GroupPositionResetMachine, GroupPositionResetMachineError,
    GroupPositionResetState, GroupPositionResetTerminal, GroupPositionResetTransition,
};

impl GroupPositionResetMachine {
    /// Applies one exact fact without I/O, clocks, retry, or Fetch activation.
    pub fn apply(
        &mut self,
        input: GroupPositionResetInput,
    ) -> Result<GroupPositionResetTransition, GroupPositionResetApplyError> {
        if input.fence() != self.fence {
            return Err(GroupPositionResetApplyError::new(
                GroupPositionResetMachineError::StaleFence,
                input,
            ));
        }
        if self.state == GroupPositionResetState::Completed {
            return Err(GroupPositionResetApplyError::new(
                GroupPositionResetMachineError::AlreadyCompleted,
                input,
            ));
        }
        if !self.accepts(input) {
            return Err(GroupPositionResetApplyError::new(
                GroupPositionResetMachineError::InvalidState,
                input,
            ));
        }
        if let Some(partition) = input.partition()
            && self.current_partition() != Some(partition)
        {
            return Err(GroupPositionResetApplyError::new(
                GroupPositionResetMachineError::StalePartition,
                input,
            ));
        }
        if let GroupPositionResetInput::DeadlineElapsed { now, .. } = input
            && !self.deadline.is_elapsed_at(now)
        {
            return Err(GroupPositionResetApplyError::new(
                GroupPositionResetMachineError::DeadlineNotElapsed,
                input,
            ));
        }

        Ok(match input {
            GroupPositionResetInput::Start { now, .. } => self.start(now),
            GroupPositionResetInput::DriverAccepted { .. } => {
                self.state = GroupPositionResetState::Submitted;
                GroupPositionResetTransition::none()
            }
            GroupPositionResetInput::DriverRejected { now, .. } => {
                self.observed_failure(now, PositionResolutionAttemptFailure::DriverRejected)
            }
            GroupPositionResetInput::OffsetResolved {
                now,
                next_offset,
                throttle_time_ms,
                ..
            } => {
                if self.deadline.is_elapsed_at(now) {
                    self.finish_failure(PositionResolutionAttemptFailure::DeadlineElapsed)
                } else {
                    self.resolve_current(next_offset, throttle_time_ms)
                }
            }
            GroupPositionResetInput::ResolutionFailed { now, failure, .. } => {
                self.observed_failure(now, failure)
            }
            GroupPositionResetInput::DeadlineElapsed { .. } => {
                self.finish_failure(PositionResolutionAttemptFailure::DeadlineElapsed)
            }
        })
    }

    const fn accepts(&self, input: GroupPositionResetInput) -> bool {
        matches!(
            (self.state, input),
            (
                GroupPositionResetState::Ready,
                GroupPositionResetInput::Start { .. }
            ) | (
                GroupPositionResetState::AwaitingDriver,
                GroupPositionResetInput::DriverAccepted { .. }
                    | GroupPositionResetInput::DriverRejected { .. }
                    | GroupPositionResetInput::DeadlineElapsed { .. }
            ) | (
                GroupPositionResetState::Submitted,
                GroupPositionResetInput::OffsetResolved { .. }
                    | GroupPositionResetInput::ResolutionFailed { .. }
                    | GroupPositionResetInput::DeadlineElapsed { .. }
            )
        )
    }

    fn start(&mut self, now: Moment) -> GroupPositionResetTransition {
        if self.deadline.is_elapsed_at(now) {
            return self.finish_failure(PositionResolutionAttemptFailure::DeadlineElapsed);
        }
        self.resolve_effect()
    }

    fn resolve_current(
        &mut self,
        next_offset: NextFetchOffset,
        throttle_time_ms: u32,
    ) -> GroupPositionResetTransition {
        let batch = self
            .batch
            .take()
            .unwrap_or_else(|| unreachable!("active reset always retains one batch"));
        let (prior_throttle_time_ms, mut facts) = batch.into_parts();
        let partition = facts[self.current_missing_index].partition();
        facts[self.current_missing_index] =
            GroupPositionPartitionFact::committed(partition, next_offset);
        self.current_missing_index = next_missing(&facts, self.current_missing_index + 1);
        self.batch = Some(GroupPositionBatch::new(
            prior_throttle_time_ms.max(throttle_time_ms),
            facts,
        ));
        if self.current_partition().is_some() {
            self.resolve_effect()
        } else {
            let batch = self
                .batch
                .take()
                .unwrap_or_else(|| unreachable!("completed reset retains one batch"));
            self.finish(GroupPositionResetTerminal::Ready(batch))
        }
    }

    fn resolve_effect(&mut self) -> GroupPositionResetTransition {
        let partition = self
            .current_partition()
            .unwrap_or_else(|| unreachable!("reset-required terminal has a missing partition"));
        self.state = GroupPositionResetState::AwaitingDriver;
        GroupPositionResetTransition::one(GroupPositionResetEffect::ResolveOffset {
            fence: self.fence,
            deadline: self.deadline,
            partition,
            position: self.position,
        })
    }

    fn observed_failure(
        &mut self,
        now: Moment,
        failure: PositionResolutionAttemptFailure,
    ) -> GroupPositionResetTransition {
        if self.deadline.is_elapsed_at(now) {
            self.finish_failure(PositionResolutionAttemptFailure::DeadlineElapsed)
        } else {
            self.finish_failure(failure)
        }
    }

    fn finish_failure(
        &mut self,
        failure: PositionResolutionAttemptFailure,
    ) -> GroupPositionResetTransition {
        let partition = self
            .current_partition()
            .unwrap_or_else(|| unreachable!("active reset has one current partition"));
        let batch = self
            .batch
            .take()
            .unwrap_or_else(|| unreachable!("active reset retains one batch"));
        self.finish(GroupPositionResetTerminal::Failed(
            GroupPositionResetFailure::new(batch, partition, failure),
        ))
    }

    fn finish(&mut self, terminal: GroupPositionResetTerminal) -> GroupPositionResetTransition {
        self.state = GroupPositionResetState::Completed;
        GroupPositionResetTransition::one(GroupPositionResetEffect::Complete {
            fence: self.fence,
            deadline: self.deadline,
            terminal,
        })
    }
}

fn next_missing(facts: &[GroupPositionPartitionFact], start: usize) -> usize {
    facts
        .iter()
        .enumerate()
        .skip(start)
        .find_map(|(index, fact)| {
            (fact.result() == GroupPositionPartitionResult::Missing).then_some(index)
        })
        .unwrap_or(facts.len())
}
