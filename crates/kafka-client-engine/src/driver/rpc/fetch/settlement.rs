//! Two-phase raw Fetch settlement and exact route-token confirmation.

use kafka_client_core::{AssignedConsumerEffect, FetchFence};
use kafka_driver::RouteFailureToken;

use super::{admission::PartitionFetchRequest, fence::supersedes, terminal::FetchTerminal};

pub(super) struct SettledFetchCall {
    fence: FetchFence,
    terminal: Option<FetchTerminal>,
    route_token: Option<RouteFailureToken>,
}

impl SettledFetchCall {
    pub(super) fn live(terminal: FetchTerminal, route_token: Option<RouteFailureToken>) -> Self {
        Self {
            fence: terminal.fence(),
            terminal: Some(terminal),
            route_token,
        }
    }

    pub(super) const fn stale(fence: FetchFence, route_token: Option<RouteFailureToken>) -> Self {
        Self {
            fence,
            terminal: None,
            route_token,
        }
    }

    pub(super) const fn fence(&self) -> FetchFence {
        self.fence
    }

    pub(super) const fn is_stale(&self) -> bool {
        self.terminal.is_none()
    }

    pub(super) fn mark_stale(
        &mut self,
        effect: AssignedConsumerEffect,
    ) -> Option<PartitionFetchRequest> {
        if !supersedes(effect, self.fence) {
            return None;
        }
        self.terminal.take().map(FetchTerminal::into_request)
    }

    #[allow(
        clippy::result_large_err,
        reason = "a stale observation must return the complete linear settled owner"
    )]
    pub(super) fn into_live_parts(
        self,
    ) -> Result<(FetchTerminal, Option<RouteFailureToken>), SettledFetchCall> {
        let Some(terminal) = self.terminal else {
            return Err(self);
        };
        Ok((terminal, self.route_token))
    }

    pub(super) fn into_request(self) -> Option<PartitionFetchRequest> {
        self.terminal.map(FetchTerminal::into_request)
    }

    #[cfg(test)]
    pub(super) fn route_kind(&self) -> Option<kafka_driver::RouteKind> {
        self.route_token.as_ref().map(RouteFailureToken::kind)
    }
}

pub(super) struct PendingFetchConfirmation {
    fence: FetchFence,
    route_token: Option<RouteFailureToken>,
}

impl PendingFetchConfirmation {
    pub(super) const fn new(fence: FetchFence, route_token: Option<RouteFailureToken>) -> Self {
        Self { fence, route_token }
    }

    pub(super) const fn fence(&self) -> FetchFence {
        self.fence
    }

    pub(super) fn into_settled(self, terminal: FetchTerminal) -> SettledFetchCall {
        SettledFetchCall::live(terminal, self.route_token)
    }

    #[cfg(test)]
    pub(super) fn route_kind(&self) -> Option<kafka_driver::RouteKind> {
        self.route_token.as_ref().map(RouteFailureToken::kind)
    }
}

/// One bounded poll observation without moving raw terminal ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchPoll {
    Idle,
    TerminalReady { fence: FetchFence },
    StaleConfirmationReady { fence: FetchFence },
}

/// Why an exact raw terminal could not begin its executor settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchBeginSettlementError {
    NoSettledCall {
        supplied: FetchFence,
    },
    StaleSettledCall {
        supplied: FetchFence,
    },
    ConfirmationPending {
        pending: FetchFence,
    },
    FenceMismatch {
        settled: FetchFence,
        supplied: FetchFence,
    },
}

/// Why exact route-token confirmation could not finish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchConfirmationError {
    NoPendingConfirmation {
        supplied: FetchFence,
    },
    FenceMismatch {
        pending: FetchFence,
        supplied: FetchFence,
    },
}

/// Why a stale terminal's retained route token could not be released.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StaleFetchConfirmationError {
    NoSettledCall {
        supplied: FetchFence,
    },
    LiveSettledCall {
        supplied: FetchFence,
    },
    FenceMismatch {
        settled: FetchFence,
        supplied: FetchFence,
    },
}

/// Failed restoration keeps the exact raw terminal outside the registry.
#[must_use = "failed Fetch restoration still owns its raw terminal"]
pub(crate) struct FetchRestoreFailure {
    terminal: FetchTerminal,
    error: FetchRestoreError,
}

impl FetchRestoreFailure {
    pub(super) const fn new(terminal: FetchTerminal, error: FetchRestoreError) -> Self {
        Self { terminal, error }
    }

    pub(crate) fn into_parts(self) -> (FetchTerminal, FetchRestoreError) {
        (self.terminal, self.error)
    }
}

/// Why a raw terminal could not rejoin its pending route confirmation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchRestoreError {
    SettledCallPresent {
        supplied: FetchFence,
    },
    NoPendingConfirmation {
        supplied: FetchFence,
    },
    FenceMismatch {
        pending: FetchFence,
        supplied: FetchFence,
    },
}
