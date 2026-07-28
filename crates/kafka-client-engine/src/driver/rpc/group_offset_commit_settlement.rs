//! Two-phase group commit input handoff and exact route-token confirmation.

use std::mem;

use kafka_client_core::{GroupOffsetCommitInput, GroupOffsetCommitPartitionResult, OperationId};
use kafka_driver::{Call, InvalidationDisposition, RouteFailureToken};

use crate::driver::DriverOwner;

pub(super) struct SettledGroupOffsetCommitCall {
    operation_id: OperationId,
    input: GroupOffsetCommitInput,
    route_token: Option<RouteFailureToken>,
    coordinator_refresh: CoordinatorRefresh,
}

impl SettledGroupOffsetCommitCall {
    pub(super) fn new(
        operation_id: OperationId,
        input: GroupOffsetCommitInput,
        mut route_token: Option<RouteFailureToken>,
    ) -> Self {
        let coordinator_refresh = match route_token_destination(&input) {
            RouteTokenDestination::Confirm => CoordinatorRefresh::None,
            RouteTokenDestination::Refresh => route_token
                .take()
                .map_or(CoordinatorRefresh::None, CoordinatorRefresh::Queued),
        };
        Self {
            operation_id,
            input,
            route_token,
            coordinator_refresh,
        }
    }

    pub(super) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(super) fn poll_coordinator_refresh(
        &mut self,
        driver: &DriverOwner,
    ) -> GroupOffsetCommitRefreshPoll {
        match mem::replace(&mut self.coordinator_refresh, CoordinatorRefresh::None) {
            CoordinatorRefresh::None => GroupOffsetCommitRefreshPoll::Ready,
            CoordinatorRefresh::Queued(route_token) => {
                match driver.driver.invalidate(route_token) {
                    Ok(call) => {
                        self.coordinator_refresh = CoordinatorRefresh::Active(call);
                        return GroupOffsetCommitRefreshPoll::Submitted;
                    }
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        self.coordinator_refresh = CoordinatorRefresh::Queued(route_token);
                    }
                }
                GroupOffsetCommitRefreshPoll::Pending
            }
            CoordinatorRefresh::Active(call) => {
                if call.try_result().is_none() {
                    self.coordinator_refresh = CoordinatorRefresh::Active(call);
                    GroupOffsetCommitRefreshPoll::Pending
                } else {
                    GroupOffsetCommitRefreshPoll::Ready
                }
            }
        }
    }

    pub(super) fn expire_coordinator_refresh(&mut self) {
        drop(mem::replace(
            &mut self.coordinator_refresh,
            CoordinatorRefresh::None,
        ));
    }

    pub(super) fn into_parts(
        self,
    ) -> (GroupOffsetCommitInput, PendingGroupOffsetCommitConfirmation) {
        (
            self.input,
            PendingGroupOffsetCommitConfirmation {
                operation_id: self.operation_id,
                route_token: self.route_token,
            },
        )
    }

    pub(super) fn recover_group_commit_after_driver_shutdown(
        self,
    ) -> (OperationId, GroupOffsetCommitInput) {
        drop(self.coordinator_refresh);
        drop(self.route_token);
        (self.operation_id, self.input)
    }
}

enum CoordinatorRefresh {
    None,
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
}

pub(super) struct PendingGroupOffsetCommitConfirmation {
    operation_id: OperationId,
    route_token: Option<RouteFailureToken>,
}

impl PendingGroupOffsetCommitConfirmation {
    pub(super) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(super) fn into_settled(
        self,
        input: GroupOffsetCommitInput,
    ) -> SettledGroupOffsetCommitCall {
        SettledGroupOffsetCommitCall::new(self.operation_id, input, self.route_token)
    }

    pub(super) fn confirm_group_commit_route_token(self) {
        drop(self.route_token);
    }

    pub(super) fn recover_group_commit_after_driver_shutdown(self) -> OperationId {
        drop(self.route_token);
        self.operation_id
    }
}

pub(super) fn needs_coordinator_refresh(input: &GroupOffsetCommitInput) -> bool {
    matches!(
        input,
        GroupOffsetCommitInput::BrokerResponded { outcomes, .. }
            if outcomes.iter().any(|outcome| {
                matches!(
                    outcome.result(),
                    GroupOffsetCommitPartitionResult::Rejected(error)
                        if matches!(error.code(), 15 | 16)
                )
            })
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RouteTokenDestination {
    Confirm,
    Refresh,
}

pub(super) fn route_token_destination(input: &GroupOffsetCommitInput) -> RouteTokenDestination {
    if needs_coordinator_refresh(input) {
        RouteTokenDestination::Refresh
    } else {
        RouteTokenDestination::Confirm
    }
}

/// One bounded nonblocking observation without moving terminal ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitPoll {
    Idle,
    TerminalReady { operation_id: OperationId },
    ConfirmationPending { operation_id: OperationId },
}

/// State of the causal coordinator-refresh barrier for one settled commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitRefreshPoll {
    Ready,
    Submitted,
    Pending,
}

/// Why terminal input ownership could not begin core settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitBeginError {
    NoSettlement {
        supplied: OperationId,
    },
    ConfirmationPending {
        pending: OperationId,
    },
    OperationMismatch {
        settled: OperationId,
        supplied: OperationId,
    },
}

/// Why exact route-token confirmation could not finish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitConfirmationError {
    NoPendingConfirmation {
        supplied: OperationId,
    },
    OperationMismatch {
        pending: OperationId,
        supplied: OperationId,
    },
}

/// Failed restoration still owns the exact normalized input.
#[must_use = "failed restoration still owns the exact group commit input"]
pub(crate) struct GroupOffsetCommitRestoreFailure {
    input: GroupOffsetCommitInput,
    error: GroupOffsetCommitRestoreError,
}

impl GroupOffsetCommitRestoreFailure {
    pub(super) const fn new(
        input: GroupOffsetCommitInput,
        error: GroupOffsetCommitRestoreError,
    ) -> Self {
        Self { input, error }
    }

    pub(crate) fn into_parts(self) -> (GroupOffsetCommitInput, GroupOffsetCommitRestoreError) {
        (self.input, self.error)
    }
}

/// Why terminal input could not rejoin its pending route confirmation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitRestoreError {
    SettlementPresent {
        supplied: OperationId,
    },
    NoPendingConfirmation {
        supplied: OperationId,
    },
    OperationMismatch {
        pending: OperationId,
        supplied: OperationId,
    },
}
