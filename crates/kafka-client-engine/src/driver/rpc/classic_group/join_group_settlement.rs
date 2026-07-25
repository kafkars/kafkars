//! Two-phase ownership of raw `JoinGroup` terminals and coordinator route tokens.

use kafka_driver::RouteFailureToken;

use super::{
    join_group_calls::AcceptedJoinGroupCall,
    join_group_terminal::{JoinGroupCallKey, JoinGroupTerminal},
};

pub(super) struct SettledJoinGroupCall {
    terminal: JoinGroupTerminal,
    route_token: Option<RouteFailureToken>,
}

impl SettledJoinGroupCall {
    pub(super) const fn new(
        terminal: JoinGroupTerminal,
        route_token: Option<RouteFailureToken>,
    ) -> Self {
        Self {
            terminal,
            route_token,
        }
    }

    pub(super) const fn key(&self) -> JoinGroupCallKey {
        self.terminal.key()
    }

    pub(super) fn into_parts(self) -> (JoinGroupTerminal, PendingJoinGroupConfirmation) {
        let key = self.terminal.key();
        (
            self.terminal,
            PendingJoinGroupConfirmation {
                key,
                route_token: self.route_token,
            },
        )
    }

    pub(super) fn recover_after_driver_shutdown(self) -> JoinGroupTerminal {
        drop(self.route_token);
        self.terminal
    }
}

pub(super) struct PendingJoinGroupConfirmation {
    key: JoinGroupCallKey,
    route_token: Option<RouteFailureToken>,
}

impl PendingJoinGroupConfirmation {
    pub(super) const fn key(&self) -> JoinGroupCallKey {
        self.key
    }

    pub(super) fn into_settled(self, terminal: JoinGroupTerminal) -> SettledJoinGroupCall {
        SettledJoinGroupCall::new(terminal, self.route_token)
    }

    pub(super) fn confirm_join_group_route_token(self) {
        drop(self.route_token);
    }

    pub(super) fn recover_after_driver_shutdown(self) -> RecoveredJoinGroupConfirmation {
        drop(self.route_token);
        RecoveredJoinGroupConfirmation { key: self.key }
    }
}

/// Pending upper-layer settlement recovered after driver shutdown.
#[must_use = "pending JoinGroup confirmation still fences an external raw terminal"]
pub(crate) struct RecoveredJoinGroupConfirmation {
    key: JoinGroupCallKey,
}

impl RecoveredJoinGroupConfirmation {
    pub(crate) const fn key(&self) -> JoinGroupCallKey {
        self.key
    }
}

/// One bounded nonblocking observation without moving terminal ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JoinGroupPoll {
    Idle,
    TerminalReady { key: JoinGroupCallKey },
    ConfirmationPending { key: JoinGroupCallKey },
}

/// Why exact raw settlement could not begin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JoinGroupBeginError {
    NoSettlement {
        supplied: JoinGroupCallKey,
    },
    ConfirmationPending {
        pending: JoinGroupCallKey,
    },
    KeyMismatch {
        settled: JoinGroupCallKey,
        supplied: JoinGroupCallKey,
    },
}

/// Why exact route-token confirmation could not finish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JoinGroupConfirmationError {
    NoPending {
        supplied: JoinGroupCallKey,
    },
    KeyMismatch {
        pending: JoinGroupCallKey,
        supplied: JoinGroupCallKey,
    },
}

/// Failed confirmation still owns the exact accepted-call receipt.
#[must_use = "failed JoinGroup confirmation still owns its accepted-call receipt"]
pub(crate) struct JoinGroupConfirmationFailure {
    accepted: AcceptedJoinGroupCall,
    error: JoinGroupConfirmationError,
}

impl JoinGroupConfirmationFailure {
    pub(super) const fn new(
        accepted: AcceptedJoinGroupCall,
        error: JoinGroupConfirmationError,
    ) -> Self {
        Self { accepted, error }
    }

    pub(crate) fn into_parts(self) -> (AcceptedJoinGroupCall, JoinGroupConfirmationError) {
        (self.accepted, self.error)
    }
}

/// Failed restoration still owns the exact raw terminal.
#[must_use = "failed JoinGroup restoration still owns its raw terminal"]
pub(crate) struct JoinGroupRestoreFailure {
    terminal: JoinGroupTerminal,
    error: JoinGroupRestoreError,
}

impl JoinGroupRestoreFailure {
    pub(super) const fn new(terminal: JoinGroupTerminal, error: JoinGroupRestoreError) -> Self {
        Self { terminal, error }
    }

    pub(crate) fn into_parts(self) -> (JoinGroupTerminal, JoinGroupRestoreError) {
        (self.terminal, self.error)
    }
}

/// Why a raw terminal could not rejoin its route-token owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JoinGroupRestoreError {
    SettlementPresent {
        supplied: JoinGroupCallKey,
    },
    NoPending {
        supplied: JoinGroupCallKey,
    },
    KeyMismatch {
        pending: JoinGroupCallKey,
        supplied: JoinGroupCallKey,
    },
}
