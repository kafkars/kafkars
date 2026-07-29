//! Neutral terminal facts for one tracked configuration-resource listing.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::ListConfigResourcesResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConfigResourcesDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum ListConfigResourcesTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a ListConfigResourcesResponse,
    },
    Failed {
        kind: ListConfigResourcesDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw ListConfigResources terminal must be deterministically settled"]
pub(crate) struct ListConfigResourcesRawTerminal {
    selected_version: Option<i16>,
    result: Result<ListConfigResourcesResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl ListConfigResourcesRawTerminal {
    pub(crate) fn fact(&self) -> ListConfigResourcesTerminalFact<'_> {
        match &self.result {
            Ok(response) => ListConfigResourcesTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => ListConfigResourcesTerminalFact::Failed {
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

pub(super) fn retain_list_config_resources_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<ListConfigResourcesResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ListConfigResourcesRawTerminal {
    ListConfigResourcesRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> ListConfigResourcesDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => ListConfigResourcesDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => ListConfigResourcesDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            ListConfigResourcesDriverFailureKind::Compatibility
        }
        _ => ListConfigResourcesDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered ListConfigResources ownership still requires settlement"]
pub(crate) struct RecoveredListConfigResourcesCall;

impl RecoveredListConfigResourcesCall {
    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self = self;
    }
}
