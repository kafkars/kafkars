//! Neutral terminal facts for one tracked feature query.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::ApiVersionsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeFeaturesDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum DescribeFeaturesTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a ApiVersionsResponse,
    },
    Failed {
        kind: DescribeFeaturesDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw DescribeFeatures terminal must be deterministically settled"]
pub(crate) struct DescribeFeaturesRawTerminal {
    selected_version: Option<i16>,
    result: Result<ApiVersionsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeFeaturesRawTerminal {
    pub(crate) fn fact(&self) -> DescribeFeaturesTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeFeaturesTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeFeaturesTerminalFact::Failed {
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

pub(super) fn retain_describe_features_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<ApiVersionsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeFeaturesRawTerminal {
    DescribeFeaturesRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeFeaturesDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeFeaturesDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeFeaturesDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeFeaturesDriverFailureKind::Compatibility
        }
        _ => DescribeFeaturesDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeFeatures ownership still requires core settlement"]
pub(crate) struct RecoveredDescribeFeaturesCall {
    _private: (),
}

impl RecoveredDescribeFeaturesCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    #[cfg(test)]
    pub(crate) const fn for_test() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
