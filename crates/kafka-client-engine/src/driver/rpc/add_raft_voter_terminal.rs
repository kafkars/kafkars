//! Neutral terminal facts for one tracked metadata-quorum voter addition.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::AddRaftVoterResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddRaftVoterDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the deterministic host interpreter.
pub(crate) enum AddRaftVoterTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a AddRaftVoterResponse,
    },
    Failed {
        kind: AddRaftVoterDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw AddRaftVoter terminal must be deterministically settled"]
pub(crate) struct AddRaftVoterRawTerminal {
    selected_version: Option<i16>,
    result: Result<AddRaftVoterResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl AddRaftVoterRawTerminal {
    pub(crate) fn fact(&self) -> AddRaftVoterTerminalFact<'_> {
        match &self.result {
            Ok(response) => AddRaftVoterTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => AddRaftVoterTerminalFact::Failed {
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

pub(super) fn retain_add_raft_voter_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<AddRaftVoterResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> AddRaftVoterRawTerminal {
    AddRaftVoterRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> AddRaftVoterDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => AddRaftVoterDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => AddRaftVoterDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => AddRaftVoterDriverFailureKind::Compatibility,
        _ => AddRaftVoterDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered AddRaftVoter ownership still requires core settlement"]
pub(crate) struct RecoveredAddRaftVoterCall;

impl RecoveredAddRaftVoterCall {
    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self = self;
    }
}
