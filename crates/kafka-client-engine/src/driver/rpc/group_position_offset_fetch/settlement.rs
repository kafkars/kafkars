//! Two-phase raw-terminal ownership and exact route-token confirmation.

use kafka_client_core::GroupPositionFence;
use kafka_driver::RouteFailureToken;

use super::{
    admission::GroupPositionOffsetFetchAccepted, terminal::GroupPositionOffsetFetchTerminal,
};

pub(super) struct SettledGroupPositionOffsetFetchCall {
    terminal: GroupPositionOffsetFetchTerminal,
    route_token: Option<RouteFailureToken>,
}

impl SettledGroupPositionOffsetFetchCall {
    pub(super) const fn new(
        terminal: GroupPositionOffsetFetchTerminal,
        route_token: Option<RouteFailureToken>,
    ) -> Self {
        Self {
            terminal,
            route_token,
        }
    }

    pub(super) const fn fence(&self) -> GroupPositionFence {
        self.terminal.key().fence()
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        GroupPositionOffsetFetchTerminal,
        PendingGroupPositionOffsetFetchConfirmation,
    ) {
        let fence = self.terminal.key().fence();
        (
            self.terminal,
            PendingGroupPositionOffsetFetchConfirmation {
                fence,
                route_token: self.route_token,
            },
        )
    }

    pub(super) fn recover_after_driver_shutdown(self) -> GroupPositionOffsetFetchTerminal {
        drop(self.route_token);
        self.terminal
    }
}

pub(super) struct PendingGroupPositionOffsetFetchConfirmation {
    fence: GroupPositionFence,
    route_token: Option<RouteFailureToken>,
}

impl PendingGroupPositionOffsetFetchConfirmation {
    pub(super) const fn fence(&self) -> GroupPositionFence {
        self.fence
    }

    pub(super) fn into_settled(
        self,
        terminal: GroupPositionOffsetFetchTerminal,
    ) -> SettledGroupPositionOffsetFetchCall {
        SettledGroupPositionOffsetFetchCall::new(terminal, self.route_token)
    }

    pub(super) fn confirm_route_token(self) {
        drop(self.route_token);
    }

    pub(super) fn recover_after_driver_shutdown(self) -> GroupPositionFence {
        drop(self.route_token);
        self.fence
    }
}

/// One bounded nonblocking observation without moving terminal ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupPositionOffsetFetchPoll {
    Idle,
    TerminalReady { fence: GroupPositionFence },
    ConfirmationPending { fence: GroupPositionFence },
}

/// Why exact raw settlement could not begin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupPositionOffsetFetchBeginError {
    NoSettlement {
        supplied: GroupPositionFence,
    },
    ConfirmationPending {
        pending: GroupPositionFence,
    },
    FenceMismatch {
        settled: GroupPositionFence,
        supplied: GroupPositionFence,
    },
}

/// Why exact route-token confirmation could not finish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupPositionOffsetFetchConfirmationError {
    NoPending {
        supplied: GroupPositionFence,
    },
    FenceMismatch {
        pending: GroupPositionFence,
        supplied: GroupPositionFence,
    },
}

/// Failed confirmation retains the exact accepted-call receipt.
#[must_use = "failed confirmation still owns group position driver acceptance"]
pub(crate) struct GroupPositionOffsetFetchConfirmationFailure {
    accepted: GroupPositionOffsetFetchAccepted,
    error: GroupPositionOffsetFetchConfirmationError,
}

impl GroupPositionOffsetFetchConfirmationFailure {
    pub(super) const fn new(
        accepted: GroupPositionOffsetFetchAccepted,
        error: GroupPositionOffsetFetchConfirmationError,
    ) -> Self {
        Self { accepted, error }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        GroupPositionOffsetFetchAccepted,
        GroupPositionOffsetFetchConfirmationError,
    ) {
        (self.accepted, self.error)
    }
}

/// Failed restoration still owns the exact raw terminal.
#[must_use = "failed restoration still owns the group position terminal"]
pub(crate) struct GroupPositionOffsetFetchRestoreFailure {
    terminal: GroupPositionOffsetFetchTerminal,
    error: GroupPositionOffsetFetchRestoreError,
}

impl GroupPositionOffsetFetchRestoreFailure {
    pub(super) const fn new(
        terminal: GroupPositionOffsetFetchTerminal,
        error: GroupPositionOffsetFetchRestoreError,
    ) -> Self {
        Self { terminal, error }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        GroupPositionOffsetFetchTerminal,
        GroupPositionOffsetFetchRestoreError,
    ) {
        (self.terminal, self.error)
    }
}

/// Why a raw terminal could not rejoin its route-token owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupPositionOffsetFetchRestoreError {
    SettlementPresent {
        supplied: GroupPositionFence,
    },
    NoPending {
        supplied: GroupPositionFence,
    },
    FenceMismatch {
        pending: GroupPositionFence,
        supplied: GroupPositionFence,
    },
}
