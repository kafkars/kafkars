//! Bounded tracked-call ownership and nonblocking group commit polling.

use kafka_client_core::GroupOffsetCommitInput;
use kafka_driver::RoutedCall;
use kafka_wire::OffsetCommitResponse;

use crate::protocol::consumer::{PreparedGroupOffsetCommit, PreparedGroupOffsetCommitRequest};

use super::{
    super::DriverOwner,
    group_offset_commit_recovery::{
        GroupOffsetCommitCompletionFailure, GroupOffsetCommitCompletionObservation,
        GroupOffsetCommitPrebuiltAdmissionFailure, GroupOffsetCommitShutdownRecovery,
        RecoveredGroupOffsetCommitSettlement,
    },
    group_offset_commit_settlement::{
        GroupOffsetCommitPoll, GroupOffsetCommitRefreshPoll, PendingGroupOffsetCommitConfirmation,
        SettledGroupOffsetCommitCall,
    },
    group_offset_commit_terminal::normalize_group_offset_commit_terminal,
};

pub(super) struct TrackedGroupOffsetCommitCall {
    prepared: PreparedGroupOffsetCommit,
    call: RoutedCall<OffsetCommitResponse>,
}

impl TrackedGroupOffsetCommitCall {
    pub(super) fn into_prepared(self) -> PreparedGroupOffsetCommit {
        let Self { prepared, call } = self;
        drop(call);
        prepared
    }
}

/// Preflighted ownership of exactly one bounded group commit call slot.
#[must_use = "a reserved group commit call slot must be submitted or released"]
pub(crate) struct GroupOffsetCommitCallPermit<'a> {
    calls: &'a mut Vec<TrackedGroupOffsetCommitCall>,
}

impl GroupOffsetCommitCallPermit<'_> {
    /// Moves the already-built generated request across the driver boundary.
    #[allow(
        clippy::result_large_err,
        reason = "driver rejection must return both exact prepared owners"
    )]
    pub(crate) fn submit_prebuilt(
        self,
        driver: &DriverOwner,
        prepared: PreparedGroupOffsetCommit,
        request: PreparedGroupOffsetCommitRequest,
    ) -> Result<GroupOffsetCommitInput, GroupOffsetCommitPrebuiltAdmissionFailure> {
        let request = request.into_generated_offset_commit_request();
        let static_membership = request.group_instance_id.is_some();
        let call = match driver.submit_tracked_group_offset_commit(
            prepared.group().as_ref(),
            request,
            prepared.operation_deadline().transport(),
            prepared.requires_leader_epoch(),
            static_membership,
            prepared.requires_consumer_group_version(),
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(GroupOffsetCommitPrebuiltAdmissionFailure::new(
                    prepared, source,
                ));
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
    #[cfg(test)]
    pub(crate) fn new(capacity: usize) -> Self {
        Self::try_new(capacity)
            .unwrap_or_else(|_error| panic!("test group commit call reservation failed"))
    }

    pub(crate) fn try_new(capacity: usize) -> Result<Self, std::collections::TryReserveError> {
        let mut calls = Vec::new();
        calls.try_reserve_exact(capacity)?;
        Ok(Self {
            capacity,
            calls,
            settled: None,
            pending_confirmation: None,
            completion_failure: None,
        })
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

    pub(crate) fn poll_group_commit_coordinator_refresh(
        &mut self,
        operation_id: kafka_client_core::OperationId,
        driver: &DriverOwner,
    ) -> Option<GroupOffsetCommitRefreshPoll> {
        self.settled
            .as_mut()
            .filter(|settled| settled.operation_id() == operation_id)
            .map(|settled| settled.poll_coordinator_refresh(driver))
    }

    pub(crate) fn expire_group_commit_coordinator_refresh(
        &mut self,
        operation_id: kafka_client_core::OperationId,
    ) -> bool {
        let Some(settled) = self
            .settled
            .as_mut()
            .filter(|settled| settled.operation_id() == operation_id)
        else {
            return false;
        };
        settled.expire_coordinator_refresh();
        true
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
        let active = core::mem::take(&mut self.calls);
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
    pub(crate) fn install_settlement_for_test(
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
