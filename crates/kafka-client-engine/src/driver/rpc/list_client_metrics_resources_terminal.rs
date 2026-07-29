//! Neutral terminal facts for one tracked client-metrics resource listing.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::ListConfigResourcesResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListClientMetricsResourcesDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum ListClientMetricsResourcesTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a ListConfigResourcesResponse,
    },
    Failed {
        kind: ListClientMetricsResourcesDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw ListClientMetricsResources terminal must be deterministically settled"]
pub(crate) struct ListClientMetricsResourcesRawTerminal {
    selected_version: Option<i16>,
    result: Result<ListConfigResourcesResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl ListClientMetricsResourcesRawTerminal {
    pub(crate) fn fact(&self) -> ListClientMetricsResourcesTerminalFact<'_> {
        match &self.result {
            Ok(response) => ListClientMetricsResourcesTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => ListClientMetricsResourcesTerminalFact::Failed {
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

pub(super) fn retain_list_client_metrics_resources_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<ListConfigResourcesResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ListClientMetricsResourcesRawTerminal {
    ListClientMetricsResourcesRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> ListClientMetricsResourcesDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => ListClientMetricsResourcesDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => ListClientMetricsResourcesDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            ListClientMetricsResourcesDriverFailureKind::Compatibility
        }
        _ => ListClientMetricsResourcesDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered ListClientMetricsResources ownership still requires core settlement"]
pub(crate) struct RecoveredListClientMetricsResourcesCall;

impl RecoveredListClientMetricsResourcesCall {
    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self = self;
    }
}
