//! Two-phase ownership of raw Heartbeat terminals and coordinator route tokens.

use kafka_driver::{RouteFailureToken, RouteKind};

use super::{
    heartbeat_calls::AcceptedClassicHeartbeatCall,
    heartbeat_terminal::{ClassicHeartbeatCallKey, ClassicHeartbeatTerminal},
};

pub(super) struct SettledClassicHeartbeatCall {
    terminal: ClassicHeartbeatTerminal,
    route_token: Option<RouteFailureToken>,
}

impl SettledClassicHeartbeatCall {
    pub(super) const fn new(
        terminal: ClassicHeartbeatTerminal,
        route_token: Option<RouteFailureToken>,
    ) -> Self {
        Self {
            terminal,
            route_token,
        }
    }

    pub(super) const fn key(&self) -> ClassicHeartbeatCallKey {
        self.terminal.key()
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        ClassicHeartbeatTerminal,
        PendingClassicHeartbeatConfirmation,
    ) {
        let key = self.terminal.key();
        (
            self.terminal,
            PendingClassicHeartbeatConfirmation {
                key,
                route_token: self.route_token,
            },
        )
    }

    pub(super) fn recover_after_driver_shutdown(self) -> ClassicHeartbeatTerminal {
        drop(self.route_token);
        self.terminal
    }
}

pub(super) struct PendingClassicHeartbeatConfirmation {
    key: ClassicHeartbeatCallKey,
    route_token: Option<RouteFailureToken>,
}

impl PendingClassicHeartbeatConfirmation {
    pub(super) const fn key(&self) -> ClassicHeartbeatCallKey {
        self.key
    }

    pub(super) fn into_settled(
        self,
        terminal: ClassicHeartbeatTerminal,
    ) -> SettledClassicHeartbeatCall {
        SettledClassicHeartbeatCall::new(terminal, self.route_token)
    }

    pub(super) fn confirm_classic_heartbeat_route_token(self) {
        drop(self.route_token);
    }

    pub(super) fn route_token_kind(&self) -> Option<RouteKind> {
        self.route_token.as_ref().map(RouteFailureToken::kind)
    }

    #[expect(
        clippy::result_large_err,
        reason = "failure returns the exact linear pending confirmation for restoration"
    )]
    pub(super) fn into_rediscovery_route_token(self) -> Result<RouteFailureToken, Self> {
        let Self { key, route_token } = self;
        match route_token {
            Some(route_token) => Ok(route_token),
            None => Err(Self {
                key,
                route_token: None,
            }),
        }
    }

    pub(super) fn recover_after_driver_shutdown(self) -> RecoveredClassicHeartbeatConfirmation {
        drop(self.route_token);
        RecoveredClassicHeartbeatConfirmation { key: self.key }
    }
}

/// Pending upper-layer settlement recovered after driver shutdown.
#[must_use = "pending Heartbeat confirmation still fences an external raw terminal"]
pub(crate) struct RecoveredClassicHeartbeatConfirmation {
    key: ClassicHeartbeatCallKey,
}

impl RecoveredClassicHeartbeatConfirmation {
    pub(crate) const fn key(&self) -> ClassicHeartbeatCallKey {
        self.key
    }
}

/// One bounded nonblocking observation without moving terminal ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicHeartbeatPoll {
    Idle,
    TerminalReady { key: ClassicHeartbeatCallKey },
    ConfirmationPending { key: ClassicHeartbeatCallKey },
}

/// Why exact raw settlement could not begin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicHeartbeatBeginError {
    NoSettlement {
        supplied: ClassicHeartbeatCallKey,
    },
    ConfirmationPending {
        pending: ClassicHeartbeatCallKey,
    },
    KeyMismatch {
        settled: ClassicHeartbeatCallKey,
        supplied: ClassicHeartbeatCallKey,
    },
}

/// Why exact route-token confirmation could not finish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicHeartbeatConfirmationError {
    NoPending {
        supplied: ClassicHeartbeatCallKey,
    },
    KeyMismatch {
        pending: ClassicHeartbeatCallKey,
        supplied: ClassicHeartbeatCallKey,
    },
    RouteTokenUnavailable {
        pending: ClassicHeartbeatCallKey,
    },
    RouteTokenKind {
        pending: ClassicHeartbeatCallKey,
        observed: RouteKind,
    },
}

/// Failed confirmation still owns the exact accepted-call receipt.
#[must_use = "failed Heartbeat confirmation still owns its accepted-call receipt"]
pub(crate) struct ClassicHeartbeatConfirmationFailure {
    accepted: AcceptedClassicHeartbeatCall,
    error: ClassicHeartbeatConfirmationError,
}

impl ClassicHeartbeatConfirmationFailure {
    pub(super) const fn new(
        accepted: AcceptedClassicHeartbeatCall,
        error: ClassicHeartbeatConfirmationError,
    ) -> Self {
        Self { accepted, error }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        AcceptedClassicHeartbeatCall,
        ClassicHeartbeatConfirmationError,
    ) {
        (self.accepted, self.error)
    }
}

/// Failed restoration still owns the exact raw terminal.
#[must_use = "failed Heartbeat restoration still owns its raw terminal"]
pub(crate) struct ClassicHeartbeatRestoreFailure {
    terminal: ClassicHeartbeatTerminal,
    error: ClassicHeartbeatRestoreError,
}

impl ClassicHeartbeatRestoreFailure {
    pub(super) const fn new(
        terminal: ClassicHeartbeatTerminal,
        error: ClassicHeartbeatRestoreError,
    ) -> Self {
        Self { terminal, error }
    }

    pub(crate) fn into_parts(self) -> (ClassicHeartbeatTerminal, ClassicHeartbeatRestoreError) {
        (self.terminal, self.error)
    }
}

/// Why a raw terminal could not rejoin its route-token owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicHeartbeatRestoreError {
    SettlementPresent {
        supplied: ClassicHeartbeatCallKey,
    },
    NoPending {
        supplied: ClassicHeartbeatCallKey,
    },
    KeyMismatch {
        pending: ClassicHeartbeatCallKey,
        supplied: ClassicHeartbeatCallKey,
    },
}
