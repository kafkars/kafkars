//! Neutral terminal facts for one tracked broker unregistration.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::UnregisterBrokerResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnregisterBrokerDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the deterministic host interpreter.
pub(crate) enum UnregisterBrokerTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a UnregisterBrokerResponse,
    },
    Failed {
        kind: UnregisterBrokerDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw UnregisterBroker terminal must be deterministically settled"]
pub(crate) struct UnregisterBrokerRawTerminal {
    selected_version: Option<i16>,
    result: Result<UnregisterBrokerResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl UnregisterBrokerRawTerminal {
    pub(crate) fn fact(&self) -> UnregisterBrokerTerminalFact<'_> {
        match &self.result {
            Ok(response) => UnregisterBrokerTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => UnregisterBrokerTerminalFact::Failed {
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

pub(super) fn retain_unregister_broker_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<UnregisterBrokerResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> UnregisterBrokerRawTerminal {
    UnregisterBrokerRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> UnregisterBrokerDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => UnregisterBrokerDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => UnregisterBrokerDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            UnregisterBrokerDriverFailureKind::Compatibility
        }
        _ => UnregisterBrokerDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered UnregisterBroker ownership still requires core settlement"]
pub(crate) struct RecoveredUnregisterBrokerCall;

impl RecoveredUnregisterBrokerCall {
    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self = self;
    }
}
