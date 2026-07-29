//! Neutral terminal facts for one tracked partition-leader producer-state call.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeProducersResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeProducersDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum DescribeProducersTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DescribeProducersResponse,
    },
    Failed {
        kind: DescribeProducersDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw DescribeProducers terminal must be deterministically settled"]
pub(crate) struct DescribeProducersRawTerminal {
    selected_version: Option<i16>,
    result: Result<DescribeProducersResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeProducersRawTerminal {
    pub(crate) fn fact(&self) -> DescribeProducersTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeProducersTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeProducersTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Releases response and route evidence only after core accepts the terminal fact.
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

pub(super) fn retain_describe_producers_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeProducersResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeProducersRawTerminal {
    DescribeProducersRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeProducersDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeProducersDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeProducersDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeProducersDriverFailureKind::Compatibility
        }
        _ => DescribeProducersDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeProducers ownership still requires core settlement"]
pub(crate) struct RecoveredDescribeProducersCall {
    _private: (),
}

impl RecoveredDescribeProducersCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    #[cfg(test)]
    pub(crate) const fn for_test() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered ownership after deterministic settlement.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
