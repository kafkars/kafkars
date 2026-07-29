//! Neutral terminal facts for one tracked feature mutation.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::UpdateFeaturesResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpdateFeaturesDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the deterministic host interpreter.
pub(crate) enum UpdateFeaturesTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a UpdateFeaturesResponse,
    },
    Failed {
        kind: UpdateFeaturesDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw UpdateFeatures terminal must be deterministically settled"]
pub(crate) struct UpdateFeaturesRawTerminal {
    selected_version: Option<i16>,
    result: Result<UpdateFeaturesResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl UpdateFeaturesRawTerminal {
    pub(crate) fn fact(&self) -> UpdateFeaturesTerminalFact<'_> {
        match &self.result {
            Ok(response) => UpdateFeaturesTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => UpdateFeaturesTerminalFact::Failed {
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

pub(super) fn retain_update_features_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<UpdateFeaturesResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> UpdateFeaturesRawTerminal {
    UpdateFeaturesRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> UpdateFeaturesDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => UpdateFeaturesDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => UpdateFeaturesDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            UpdateFeaturesDriverFailureKind::Compatibility
        }
        _ => UpdateFeaturesDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered UpdateFeatures ownership still requires core settlement"]
pub(crate) struct RecoveredUpdateFeaturesCall;

impl RecoveredUpdateFeaturesCall {
    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self = self;
    }
}
