//! One bounded scheduling action for queued and driver-owned commits.

use kafka_client_core::{DeliveryStatus, GroupOffsetCommitInput, Moment};

use crate::driver::{DriverOwner, GroupOffsetCommitPoll};

use super::host::{
    GroupOffsetCommitAttempt, GroupOffsetCommitHost, GroupOffsetCommitHostError,
    GroupOffsetCommitTurn,
};

impl GroupOffsetCommitHost {
    pub(in crate::consumer::group) fn turn(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<GroupOffsetCommitTurn, GroupOffsetCommitHostError> {
        match self.turn_once(now, driver) {
            Ok(turn) => Ok(turn),
            Err(error) => {
                if !matches!(
                    error,
                    GroupOffsetCommitHostError::Completion(
                        crate::completion::CompletionRegistryError::NotificationBackpressure
                    )
                ) {
                    self.fault = Some(error);
                }
                Err(error)
            }
        }
    }

    fn turn_once(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<GroupOffsetCommitTurn, GroupOffsetCommitHostError> {
        if let Some(fault) = self.fault {
            return Err(fault);
        }
        if self.reclaim_one()? {
            return Ok(GroupOffsetCommitTurn::Progress);
        }
        if let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.terminal.is_some())
        {
            self.publish_terminal(index)?;
            return Ok(GroupOffsetCommitTurn::Progress);
        }
        match self.calls.poll_group_commit() {
            Ok(GroupOffsetCommitPoll::TerminalReady { operation_id }) => {
                self.settle_driver_terminal(operation_id)?;
                return Ok(GroupOffsetCommitTurn::Progress);
            }
            Ok(GroupOffsetCommitPoll::ConfirmationPending { .. }) => {
                return Err(GroupOffsetCommitHostError::Settlement);
            }
            Err(_observation) => return Err(GroupOffsetCommitHostError::DriverCompletion),
            Ok(GroupOffsetCommitPoll::Idle) => {}
        }
        if let Some(index) = self.expired_queued(now) {
            self.apply_terminal(
                index,
                GroupOffsetCommitInput::DeadlineElapsed {
                    delivery: DeliveryStatus::NotSent,
                },
                super::host::GroupOffsetCommitSettlementProvenance::DefinitelyUnsent,
            )?;
            self.operations[index].replace_attempt(None);
            self.publish_terminal(index)?;
            return Ok(GroupOffsetCommitTurn::Progress);
        }
        let Some(index) = self.queued_index() else {
            return Ok(GroupOffsetCommitTurn::Idle);
        };
        if self.submit(index, driver)? {
            Ok(GroupOffsetCommitTurn::Progress)
        } else {
            Ok(GroupOffsetCommitTurn::Idle)
        }
    }

    pub(in crate::consumer::group) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.operations
            .iter()
            .filter(|operation| {
                matches!(operation.attempt, Some(GroupOffsetCommitAttempt::Queued(_)))
            })
            .map(|operation| operation.deadline.core())
            .min()
    }

    fn expired_queued(&self, now: Moment) -> Option<usize> {
        self.operations.iter().position(|operation| {
            matches!(operation.attempt, Some(GroupOffsetCommitAttempt::Queued(_)))
                && operation.deadline.core().is_elapsed_at(now)
        })
    }

    fn queued_index(&self) -> Option<usize> {
        self.operations.iter().position(|operation| {
            matches!(operation.attempt, Some(GroupOffsetCommitAttempt::Queued(_)))
        })
    }

    fn submit(
        &mut self,
        index: usize,
        driver: &DriverOwner,
    ) -> Result<bool, GroupOffsetCommitHostError> {
        let Some(permit) = self.calls.try_reserve_group_commit() else {
            return Ok(false);
        };
        let Some(GroupOffsetCommitAttempt::Queued(submission)) =
            self.operations[index].replace_attempt(None)
        else {
            return Err(GroupOffsetCommitHostError::MissingPrepared);
        };
        self.operations[index].replace_attempt(Some(GroupOffsetCommitAttempt::HandedOff));
        match permit.submit_prebuilt(driver, submission.prepared, submission.request) {
            Ok(input) => {
                self.apply_nonterminal(index, input)?;
                Ok(true)
            }
            Err(failure) => {
                let (prepared, input, _source) = failure.into_parts();
                self.operations[index]
                    .replace_attempt(Some(GroupOffsetCommitAttempt::Recovery(prepared)));
                self.apply_terminal(
                    index,
                    input,
                    super::host::GroupOffsetCommitSettlementProvenance::DefinitelyUnsent,
                )?;
                self.operations[index].replace_attempt(None);
                self.publish_terminal(index)?;
                Ok(true)
            }
        }
    }
}
