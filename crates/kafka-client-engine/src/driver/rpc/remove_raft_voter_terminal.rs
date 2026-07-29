//! Neutral terminal facts for one tracked metadata-quorum voter removal.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::RemoveRaftVoterResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemoveRaftVoterDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the deterministic host interpreter.
pub(crate) enum RemoveRaftVoterTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a RemoveRaftVoterResponse,
    },
    Failed {
        kind: RemoveRaftVoterDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw RemoveRaftVoter terminal must be deterministically settled"]
pub(crate) struct RemoveRaftVoterRawTerminal {
    selected_version: Option<i16>,
    result: Result<RemoveRaftVoterResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl RemoveRaftVoterRawTerminal {
    pub(crate) fn fact(&self) -> RemoveRaftVoterTerminalFact<'_> {
        match &self.result {
            Ok(response) => RemoveRaftVoterTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => RemoveRaftVoterTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Releases response and route evidence only after deterministic settlement.
    pub(crate) fn discard(self) {
        let Self {
            selected_version: _,
            result,
            route_token,
        } = self;
        drop(result);
        drop(route_token);
    }
}

pub(super) fn retain_remove_raft_voter_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<RemoveRaftVoterResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> RemoveRaftVoterRawTerminal {
    RemoveRaftVoterRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> RemoveRaftVoterDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => RemoveRaftVoterDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => RemoveRaftVoterDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            RemoveRaftVoterDriverFailureKind::Compatibility
        }
        _ => RemoveRaftVoterDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered RemoveRaftVoter ownership still requires core settlement"]
pub(crate) struct RecoveredRemoveRaftVoterCall;

impl RecoveredRemoveRaftVoterCall {
    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self = self;
    }
}
