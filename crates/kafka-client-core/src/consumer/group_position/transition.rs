//! Atomic bootstrap start, correlation, and terminal assignment.

use super::{
    GroupPositionBootstrapApplyError, GroupPositionBootstrapEffect, GroupPositionBootstrapFailure,
    GroupPositionBootstrapFailureKind, GroupPositionBootstrapFetchFailure,
    GroupPositionBootstrapInput, GroupPositionBootstrapMachine, GroupPositionBootstrapMachineError,
    GroupPositionBootstrapMissingOffsets, GroupPositionBootstrapPartitionRejection,
    GroupPositionBootstrapState, GroupPositionBootstrapTerminal, GroupPositionBootstrapTransition,
    GroupPositionPartitionResult,
};

impl GroupPositionBootstrapMachine {
    /// Applies one exact normalized fact without I/O, clocks, retry, or Fetch activation.
    pub fn apply(
        &mut self,
        input: GroupPositionBootstrapInput,
    ) -> Result<GroupPositionBootstrapTransition, GroupPositionBootstrapApplyError> {
        let supplied = input.fence();
        if supplied != self.fence {
            return Err(GroupPositionBootstrapApplyError::new(
                GroupPositionBootstrapMachineError::StaleFence,
                input,
            ));
        }
        if self.state == GroupPositionBootstrapState::Completed {
            return Err(GroupPositionBootstrapApplyError::new(
                GroupPositionBootstrapMachineError::AlreadyCompleted,
                input,
            ));
        }
        if !self.accepts(&input) {
            return Err(GroupPositionBootstrapApplyError::new(
                GroupPositionBootstrapMachineError::InvalidState,
                input,
            ));
        }
        if let GroupPositionBootstrapInput::DeadlineElapsed { now, .. } = &input
            && !self.deadline.is_elapsed_at(*now)
        {
            return Err(GroupPositionBootstrapApplyError::new(
                GroupPositionBootstrapMachineError::DeadlineNotElapsed,
                input,
            ));
        }

        Ok(match input {
            GroupPositionBootstrapInput::Start { now, .. } => self.start(now),
            GroupPositionBootstrapInput::DriverAccepted { .. } => {
                self.state = GroupPositionBootstrapState::Submitted;
                GroupPositionBootstrapTransition::none()
            }
            GroupPositionBootstrapInput::DriverRejected { now, .. } => {
                self.observed_failure(now, GroupPositionBootstrapFailureKind::DriverRejected)
            }
            GroupPositionBootstrapInput::DeadlineElapsed { .. } => {
                self.finish_failure(GroupPositionBootstrapFailureKind::DeadlineElapsed)
            }
            GroupPositionBootstrapInput::BrokerRejected { now, error, .. } => {
                self.observed_failure(now, GroupPositionBootstrapFailureKind::Broker(error))
            }
            GroupPositionBootstrapInput::OffsetsFetched { now, batch, .. } => {
                if self.deadline.is_elapsed_at(now) {
                    self.finish_failure(GroupPositionBootstrapFailureKind::DeadlineElapsed)
                } else {
                    self.offsets_fetched(batch)
                }
            }
            GroupPositionBootstrapInput::FetchFailed { now, failure, .. } => {
                self.observed_failure(now, map_fetch_failure(failure))
            }
        })
    }

    fn accepts(&self, input: &GroupPositionBootstrapInput) -> bool {
        matches!(
            (self.state, input),
            (
                GroupPositionBootstrapState::Ready,
                GroupPositionBootstrapInput::Start { .. }
            ) | (
                GroupPositionBootstrapState::AwaitingDriver,
                GroupPositionBootstrapInput::DriverAccepted { .. }
                    | GroupPositionBootstrapInput::DriverRejected { .. }
                    | GroupPositionBootstrapInput::DeadlineElapsed { .. }
            ) | (
                GroupPositionBootstrapState::Submitted,
                GroupPositionBootstrapInput::DeadlineElapsed { .. }
                    | GroupPositionBootstrapInput::BrokerRejected { .. }
                    | GroupPositionBootstrapInput::OffsetsFetched { .. }
                    | GroupPositionBootstrapInput::FetchFailed { .. }
            )
        )
    }

    fn start(&mut self, now: crate::Moment) -> GroupPositionBootstrapTransition {
        if self.deadline.is_elapsed_at(now) {
            return self.finish_failure(GroupPositionBootstrapFailureKind::DeadlineElapsed);
        }
        let partitions = core::mem::take(&mut self.request_partitions);
        if partitions.is_empty() {
            return self.finish(GroupPositionBootstrapTerminal::Ready(
                super::GroupPositionBatch::new(0, Vec::new()),
            ));
        }
        self.state = GroupPositionBootstrapState::AwaitingDriver;
        GroupPositionBootstrapTransition::one(GroupPositionBootstrapEffect::FetchOffsets {
            fence: self.fence,
            deadline: self.deadline,
            partitions,
        })
    }

    fn observed_failure(
        &mut self,
        now: crate::Moment,
        kind: GroupPositionBootstrapFailureKind,
    ) -> GroupPositionBootstrapTransition {
        if self.deadline.is_elapsed_at(now) {
            self.finish_failure(GroupPositionBootstrapFailureKind::DeadlineElapsed)
        } else {
            self.finish_failure(kind)
        }
    }

    fn offsets_fetched(
        &mut self,
        batch: super::GroupPositionBatch,
    ) -> GroupPositionBootstrapTransition {
        if self.expected.len() != batch.facts().len()
            || self
                .expected
                .iter()
                .zip(batch.facts())
                .any(|(expected, fact)| *expected != fact.partition())
        {
            return self.finish_failure(GroupPositionBootstrapFailureKind::InvalidResponse);
        }
        if let Some(index) = batch
            .facts()
            .iter()
            .position(|fact| matches!(fact.result(), GroupPositionPartitionResult::Rejected(_)))
        {
            return self.finish(GroupPositionBootstrapTerminal::PartitionRejected(
                GroupPositionBootstrapPartitionRejection::new(batch, index),
            ));
        }
        if let Some(index) = batch
            .facts()
            .iter()
            .position(|fact| fact.result() == GroupPositionPartitionResult::Missing)
        {
            return self.finish(GroupPositionBootstrapTerminal::MissingOffsets(
                GroupPositionBootstrapMissingOffsets::new(batch, index),
            ));
        }
        self.finish(GroupPositionBootstrapTerminal::Ready(batch))
    }

    fn finish_failure(
        &mut self,
        kind: GroupPositionBootstrapFailureKind,
    ) -> GroupPositionBootstrapTransition {
        self.finish(GroupPositionBootstrapTerminal::Failed(
            GroupPositionBootstrapFailure::new(kind),
        ))
    }

    fn finish(
        &mut self,
        terminal: GroupPositionBootstrapTerminal,
    ) -> GroupPositionBootstrapTransition {
        self.state = GroupPositionBootstrapState::Completed;
        GroupPositionBootstrapTransition::one(GroupPositionBootstrapEffect::Complete {
            fence: self.fence,
            deadline: self.deadline,
            terminal,
        })
    }
}

const fn map_fetch_failure(
    failure: GroupPositionBootstrapFetchFailure,
) -> GroupPositionBootstrapFailureKind {
    match failure {
        GroupPositionBootstrapFetchFailure::Transport => {
            GroupPositionBootstrapFailureKind::Transport
        }
        GroupPositionBootstrapFetchFailure::Compatibility => {
            GroupPositionBootstrapFailureKind::Compatibility
        }
        GroupPositionBootstrapFetchFailure::InvalidResponse => {
            GroupPositionBootstrapFailureKind::InvalidResponse
        }
        GroupPositionBootstrapFetchFailure::ResponseTooLarge => {
            GroupPositionBootstrapFailureKind::ResponseTooLarge
        }
    }
}
