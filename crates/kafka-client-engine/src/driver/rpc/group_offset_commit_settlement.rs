//! Two-phase group commit input handoff and exact route-token confirmation.

use kafka_client_core::{GroupOffsetCommitInput, OperationId};
use kafka_driver::RouteFailureToken;

pub(super) struct SettledGroupOffsetCommitCall {
    operation_id: OperationId,
    input: GroupOffsetCommitInput,
    route_token: Option<RouteFailureToken>,
}

impl SettledGroupOffsetCommitCall {
    pub(super) const fn new(
        operation_id: OperationId,
        input: GroupOffsetCommitInput,
        route_token: Option<RouteFailureToken>,
    ) -> Self {
        Self {
            operation_id,
            input,
            route_token,
        }
    }

    pub(super) const fn operation_id(&self) -> OperationId {
        self.operation_id
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
        drop(self.route_token);
        (self.operation_id, self.input)
    }
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

/// One bounded nonblocking observation without moving terminal ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitPoll {
    Idle,
    TerminalReady { operation_id: OperationId },
    ConfirmationPending { operation_id: OperationId },
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
