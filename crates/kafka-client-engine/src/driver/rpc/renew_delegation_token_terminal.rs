//! Neutral terminal facts for one tracked delegation-token renewal.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::RenewDelegationTokenResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RenewDelegationTokenDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or failure for the deterministic host interpreter.
pub(crate) enum RenewDelegationTokenTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a RenewDelegationTokenResponse,
    },
    Failed {
        kind: RenewDelegationTokenDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response and route evidence retained through deterministic settlement.
#[must_use = "a raw RenewDelegationToken terminal must be deterministically settled"]
pub(crate) struct RenewDelegationTokenRawTerminal {
    selected_version: Option<i16>,
    result: Result<RenewDelegationTokenResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl RenewDelegationTokenRawTerminal {
    pub(crate) fn fact(&self) -> RenewDelegationTokenTerminalFact<'_> {
        match &self.result {
            Ok(response) => RenewDelegationTokenTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => RenewDelegationTokenTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Releases response and route evidence only after core settlement.
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

pub(super) fn retain_renew_delegation_token_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<RenewDelegationTokenResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> RenewDelegationTokenRawTerminal {
    RenewDelegationTokenRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> RenewDelegationTokenDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => RenewDelegationTokenDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => RenewDelegationTokenDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            RenewDelegationTokenDriverFailureKind::Compatibility
        }
        _ => RenewDelegationTokenDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered RenewDelegationToken ownership still requires core settlement"]
pub(crate) struct RecoveredRenewDelegationTokenCall;

impl RecoveredRenewDelegationTokenCall {
    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self = self;
    }
}
