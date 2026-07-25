//! Bounded tracked-call ownership and nonblocking group commit polling.

use kafka_client_core::GroupOffsetCommitInput;
use kafka_driver::RoutedCall;
use kafka_wire::OffsetCommitResponse;

use crate::protocol::consumer::{PreparedGroupOffsetCommit, group_offset_commit_request};

use super::{
    super::DriverOwner,
    group_offset_commit_recovery::{
        GroupOffsetCommitAdmissionFailure, GroupOffsetCommitCompletionFailure,
        GroupOffsetCommitCompletionObservation, GroupOffsetCommitShutdownRecovery,
        RecoveredGroupOffsetCommitSettlement,
    },
    group_offset_commit_settlement::{
        GroupOffsetCommitPoll, PendingGroupOffsetCommitConfirmation, SettledGroupOffsetCommitCall,
    },
    group_offset_commit_terminal::normalize_group_offset_commit_terminal,
};

pub(super) struct TrackedGroupOffsetCommitCall {
    prepared: PreparedGroupOffsetCommit,
    call: RoutedCall<OffsetCommitResponse>,
}

/// Preflighted ownership of exactly one bounded group commit call slot.
#[must_use = "a reserved group commit call slot must be submitted or released"]
pub(crate) struct GroupOffsetCommitCallPermit<'a> {
    calls: &'a mut Vec<TrackedGroupOffsetCommitCall>,
}

impl GroupOffsetCommitCallPermit<'_> {
    /// Submits after the future group owner has checked the core deadline at
    /// the immediate handoff boundary; this clock-free lane preserves only
    /// the already-captured transport `Instant`.
    #[allow(
        clippy::result_large_err,
        reason = "driver rejection must return the exact prepared group commit owner"
    )]
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        prepared: PreparedGroupOffsetCommit,
    ) -> Result<GroupOffsetCommitInput, GroupOffsetCommitAdmissionFailure> {
        let generated = group_offset_commit_request(&prepared);
        let call = match driver.submit_tracked_group_offset_commit(
            prepared.group().as_ref(),
            generated,
            prepared.operation_deadline().transport(),
            prepared.requires_leader_epoch(),
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(GroupOffsetCommitAdmissionFailure::new(prepared, source));
            }
        };
        self.calls
            .push(TrackedGroupOffsetCommitCall { prepared, call });
        Ok(GroupOffsetCommitInput::DriverAccepted)
    }
}

/// Capacity-bounded registry of active, settled, confirming, and corrupted calls.
pub(crate) struct TrackedGroupOffsetCommitCalls {
    pub(super) capacity: usize,
    pub(super) calls: Vec<TrackedGroupOffsetCommitCall>,
    pub(super) settled: Option<SettledGroupOffsetCommitCall>,
    pub(super) pending_confirmation: Option<PendingGroupOffsetCommitConfirmation>,
    completion_failure: Option<GroupOffsetCommitCompletionFailure>,
}

impl TrackedGroupOffsetCommitCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
            pending_confirmation: None,
            completion_failure: None,
        }
    }

    pub(crate) fn try_reserve_group_commit(&mut self) -> Option<GroupOffsetCommitCallPermit<'_>> {
        if self.retained_group_commit_count() >= self.capacity {
            return None;
        }
        Some(GroupOffsetCommitCallPermit {
            calls: &mut self.calls,
        })
    }

    pub(crate) fn retained_group_commit_count(&self) -> usize {
        self.calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(usize::from(self.pending_confirmation.is_some()))
            .saturating_add(usize::from(self.completion_failure.is_some()))
    }

    pub(crate) fn poll_group_commit(
        &mut self,
    ) -> Result<GroupOffsetCommitPoll, GroupOffsetCommitCompletionObservation> {
        if let Some(failure) = &self.completion_failure {
            return Err(failure.observation());
        }
        if let Some(pending) = &self.pending_confirmation {
            return Ok(GroupOffsetCommitPoll::ConfirmationPending {
                operation_id: pending.operation_id(),
            });
        }
        if let Some(settled) = &self.settled {
            return Ok(GroupOffsetCommitPoll::TerminalReady {
                operation_id: settled.operation_id(),
            });
        }
        let Some((index, result)) = self
            .calls
            .iter()
            .enumerate()
            .find_map(|(index, call)| call.call.try_result().map(|result| (index, result)))
        else {
            return Ok(GroupOffsetCommitPoll::Idle);
        };
        let tracked = self.calls.remove(index);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(source) => {
                let failure = GroupOffsetCommitCompletionFailure::new(tracked.prepared, source);
                let observation = failure.observation();
                self.completion_failure = Some(failure);
                return Err(observation);
            }
        };
        let operation_id = tracked.prepared.operation_id();
        let (result, selected_version, route_token) = outcome.into_parts();
        let input =
            normalize_group_offset_commit_terminal(tracked.prepared, selected_version, result);
        self.settled = Some(SettledGroupOffsetCommitCall::new(
            operation_id,
            input,
            route_token,
        ));
        Ok(GroupOffsetCommitPoll::TerminalReady { operation_id })
    }

    pub(crate) fn recover_group_commits_after_driver_shutdown(
        &mut self,
    ) -> GroupOffsetCommitShutdownRecovery {
        let active = self
            .calls
            .drain(..)
            .map(|tracked| tracked.prepared)
            .collect();
        let settled = self.settled.take().map(|settled| {
            let (operation_id, input) = settled.recover_group_commit_after_driver_shutdown();
            RecoveredGroupOffsetCommitSettlement::new(operation_id, input)
        });
        let pending_operation_id = self
            .pending_confirmation
            .take()
            .map(PendingGroupOffsetCommitConfirmation::recover_group_commit_after_driver_shutdown);
        let completion = self
            .completion_failure
            .take()
            .map(GroupOffsetCommitCompletionFailure::into_recovery);
        GroupOffsetCommitShutdownRecovery::new(active, settled, pending_operation_id, completion)
    }

    #[cfg(test)]
    pub(super) fn install_settlement_for_test(
        &mut self,
        operation_id: kafka_client_core::OperationId,
        input: GroupOffsetCommitInput,
    ) {
        self.settled = Some(SettledGroupOffsetCommitCall::new(operation_id, input, None));
    }

    #[cfg(test)]
    pub(super) fn install_completion_failure_for_test(
        &mut self,
        prepared: PreparedGroupOffsetCommit,
        source: kafka_driver::CompletionError,
    ) {
        self.completion_failure = Some(GroupOffsetCommitCompletionFailure::new(prepared, source));
    }
}
