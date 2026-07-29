//! Neutral API-77 terminal facts with linear coordinator-route evidence.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::ShareGroupDescribeResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification independent of driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeShareGroupDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum DescribeShareGroupTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a ShareGroupDescribeResponse,
    },
    Failed {
        kind: DescribeShareGroupDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained through normalization and core settlement.
#[must_use = "a raw API-77 terminal owns unsettled route evidence"]
pub(crate) struct DescribeShareGroupTerminal {
    selected_version: Option<i16>,
    result: Result<ShareGroupDescribeResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeShareGroupTerminal {
    pub(crate) fn fact(&self) -> DescribeShareGroupTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeShareGroupTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeShareGroupTerminalFact::Failed {
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

pub(super) fn retain_describe_share_group_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<ShareGroupDescribeResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeShareGroupTerminal {
    DescribeShareGroupTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeShareGroupDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeShareGroupDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeShareGroupDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeShareGroupDriverFailureKind::Compatibility
        }
        _ => DescribeShareGroupDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after unique driver destruction.
#[must_use = "recovered API-77 ownership still requires deterministic settlement"]
pub(crate) struct RecoveredDescribeShareGroupCall {
    _private: (),
}

impl RecoveredDescribeShareGroupCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    #[cfg(test)]
    pub(crate) const fn for_test() -> Self {
        Self::new()
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
