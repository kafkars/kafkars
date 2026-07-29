//! Neutral API-89 terminal facts with linear coordinator-route evidence.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::StreamsGroupDescribeResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification independent of driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeStreamsGroupDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum DescribeStreamsGroupTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a StreamsGroupDescribeResponse,
    },
    Failed {
        kind: DescribeStreamsGroupDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained through normalization and core settlement.
#[must_use = "a raw API-89 terminal owns unsettled route evidence"]
pub(crate) struct DescribeStreamsGroupTerminal {
    selected_version: Option<i16>,
    result: Result<StreamsGroupDescribeResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeStreamsGroupTerminal {
    pub(crate) fn fact(&self) -> DescribeStreamsGroupTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeStreamsGroupTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeStreamsGroupTerminalFact::Failed {
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

pub(super) fn retain_describe_streams_group_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<StreamsGroupDescribeResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeStreamsGroupTerminal {
    DescribeStreamsGroupTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeStreamsGroupDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeStreamsGroupDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeStreamsGroupDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeStreamsGroupDriverFailureKind::Compatibility
        }
        _ => DescribeStreamsGroupDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after unique driver destruction.
#[must_use = "recovered API-89 ownership still requires deterministic settlement"]
pub(crate) struct RecoveredDescribeStreamsGroupCall {
    _private: (),
}

impl RecoveredDescribeStreamsGroupCall {
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
