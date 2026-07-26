//! Two-phase ownership of raw `SyncGroup` terminals and coordinator route tokens.

use kafka_driver::{RouteFailureToken, RouteKind};

use super::{
    sync_group_calls::AcceptedSyncGroupCall,
    sync_group_terminal::{SyncGroupCallKey, SyncGroupTerminal},
};

pub(super) struct SettledSyncGroupCall {
    terminal: SyncGroupTerminal,
    route_token: Option<RouteFailureToken>,
}

impl SettledSyncGroupCall {
    pub(super) const fn new(
        terminal: SyncGroupTerminal,
        route_token: Option<RouteFailureToken>,
    ) -> Self {
        Self {
            terminal,
            route_token,
        }
    }

    pub(super) const fn key(&self) -> SyncGroupCallKey {
        self.terminal.key()
    }

    pub(super) fn into_parts(self) -> (SyncGroupTerminal, PendingSyncGroupConfirmation) {
        let key = self.terminal.key();
        (
            self.terminal,
            PendingSyncGroupConfirmation {
                key,
                route_token: self.route_token,
            },
        )
    }

    pub(super) fn recover_after_driver_shutdown(self) -> SyncGroupTerminal {
        drop(self.route_token);
        self.terminal
    }
}

pub(super) struct PendingSyncGroupConfirmation {
    key: SyncGroupCallKey,
    route_token: Option<RouteFailureToken>,
}

impl PendingSyncGroupConfirmation {
    pub(super) const fn key(&self) -> SyncGroupCallKey {
        self.key
    }

    pub(super) fn into_settled(self, terminal: SyncGroupTerminal) -> SettledSyncGroupCall {
        SettledSyncGroupCall::new(terminal, self.route_token)
    }

    pub(super) fn confirm_sync_group_route_token(self) {
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

    pub(super) fn recover_after_driver_shutdown(self) -> RecoveredSyncGroupConfirmation {
        drop(self.route_token);
        RecoveredSyncGroupConfirmation { key: self.key }
    }
}

/// Pending upper-layer settlement recovered after driver shutdown.
#[must_use = "pending SyncGroup confirmation still fences an external raw terminal"]
pub(crate) struct RecoveredSyncGroupConfirmation {
    key: SyncGroupCallKey,
}

impl RecoveredSyncGroupConfirmation {
    pub(crate) const fn key(&self) -> SyncGroupCallKey {
        self.key
    }
}

/// One bounded nonblocking observation without moving terminal ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncGroupPoll {
    Idle,
    TerminalReady { key: SyncGroupCallKey },
    ConfirmationPending { key: SyncGroupCallKey },
}

/// Why exact raw settlement could not begin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncGroupBeginError {
    NoSettlement {
        supplied: SyncGroupCallKey,
    },
    ConfirmationPending {
        pending: SyncGroupCallKey,
    },
    KeyMismatch {
        settled: SyncGroupCallKey,
        supplied: SyncGroupCallKey,
    },
}

/// Why exact route-token confirmation could not finish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncGroupConfirmationError {
    NoPending {
        supplied: SyncGroupCallKey,
    },
    KeyMismatch {
        pending: SyncGroupCallKey,
        supplied: SyncGroupCallKey,
    },
    RouteTokenUnavailable {
        pending: SyncGroupCallKey,
    },
    RouteTokenKind {
        pending: SyncGroupCallKey,
        observed: RouteKind,
    },
}

/// Failed confirmation still owns the exact accepted-call receipt.
#[must_use = "failed SyncGroup confirmation still owns its accepted-call receipt"]
pub(crate) struct SyncGroupConfirmationFailure {
    accepted: AcceptedSyncGroupCall,
    error: SyncGroupConfirmationError,
}

impl SyncGroupConfirmationFailure {
    pub(super) const fn new(
        accepted: AcceptedSyncGroupCall,
        error: SyncGroupConfirmationError,
    ) -> Self {
        Self { accepted, error }
    }

    pub(crate) fn into_parts(self) -> (AcceptedSyncGroupCall, SyncGroupConfirmationError) {
        (self.accepted, self.error)
    }
}

/// Failed restoration still owns the exact raw terminal.
#[must_use = "failed SyncGroup restoration still owns its raw terminal"]
pub(crate) struct SyncGroupRestoreFailure {
    terminal: SyncGroupTerminal,
    error: SyncGroupRestoreError,
}

impl SyncGroupRestoreFailure {
    pub(super) const fn new(terminal: SyncGroupTerminal, error: SyncGroupRestoreError) -> Self {
        Self { terminal, error }
    }

    pub(crate) fn into_parts(self) -> (SyncGroupTerminal, SyncGroupRestoreError) {
        (self.terminal, self.error)
    }
}

/// Why a raw terminal could not rejoin its route-token owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncGroupRestoreError {
    SettlementPresent {
        supplied: SyncGroupCallKey,
    },
    NoPending {
        supplied: SyncGroupCallKey,
    },
    KeyMismatch {
        pending: SyncGroupCallKey,
        supplied: SyncGroupCallKey,
    },
}
