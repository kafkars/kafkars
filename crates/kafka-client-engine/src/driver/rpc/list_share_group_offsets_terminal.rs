//! Neutral API-90 terminal facts with linear group-coordinator route evidence.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeShareGroupOffsetsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local failure class independent of driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListShareGroupOffsetsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum ListShareGroupOffsetsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DescribeShareGroupOffsetsResponse,
    },
    Failed {
        kind: ListShareGroupOffsetsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained through normalization and core settlement.
#[must_use = "a raw API-90 terminal owns unsettled route evidence"]
pub(crate) struct ListShareGroupOffsetsTerminal {
    selected_version: Option<i16>,
    result: Result<DescribeShareGroupOffsetsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl ListShareGroupOffsetsTerminal {
    pub(crate) fn fact(&self) -> ListShareGroupOffsetsTerminalFact<'_> {
        match &self.result {
            Ok(response) => ListShareGroupOffsetsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => ListShareGroupOffsetsTerminalFact::Failed {
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

pub(super) fn retain_list_share_group_offsets_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeShareGroupOffsetsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ListShareGroupOffsetsTerminal {
    ListShareGroupOffsetsTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> ListShareGroupOffsetsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => ListShareGroupOffsetsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => ListShareGroupOffsetsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            ListShareGroupOffsetsDriverFailureKind::Compatibility
        }
        _ => ListShareGroupOffsetsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after unique driver destruction.
#[must_use = "recovered API-90 ownership still requires deterministic settlement"]
pub(crate) struct RecoveredListShareGroupOffsetsCall {
    _private: (),
}

impl RecoveredListShareGroupOffsetsCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
