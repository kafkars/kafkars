//! Raw election terminal facts with linear route-receipt ownership.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::ElectLeadersResponse;

use super::super::request_failure_delivery;

/// Stable engine-local failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ElectLeadersDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure safe for the concrete host interpreter.
pub(crate) enum ElectLeadersTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a ElectLeadersResponse,
    },
    Failed {
        kind: ElectLeadersDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained until deterministic settlement.
#[must_use = "a raw election terminal owns unsettled route evidence"]
pub(crate) struct ElectLeadersTerminal {
    selected_version: Option<i16>,
    result: Result<ElectLeadersResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl ElectLeadersTerminal {
    pub(crate) fn fact(&self) -> ElectLeadersTerminalFact<'_> {
        match &self.result {
            Ok(response) => ElectLeadersTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => ElectLeadersTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Releases route evidence only after core terminal settlement.
    pub(crate) fn discard(self) {
        drop(self.route_token);
    }
}

pub(super) fn retain_elect_leaders_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<ElectLeadersResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ElectLeadersTerminal {
    ElectLeadersTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> ElectLeadersDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => ElectLeadersDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => ElectLeadersDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => ElectLeadersDriverFailureKind::Compatibility,
        _ => ElectLeadersDriverFailureKind::Transport,
    }
}

/// Accepted call recovered only after the unique driver is destroyed.
#[must_use = "recovered election ownership still requires core settlement"]
pub(crate) struct RecoveredElectLeadersCall {
    _private: (),
}

impl RecoveredElectLeadersCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
