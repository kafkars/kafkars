//! Neutral terminal facts for one tracked AnyBroker `DescribeClientQuotas` call.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeClientQuotasResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeClientQuotasDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum DescribeClientQuotasTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DescribeClientQuotasResponse,
    },
    Failed {
        kind: DescribeClientQuotasDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained through protocol validation and core settlement.
#[must_use = "a raw DescribeClientQuotas terminal must be deterministically settled"]
pub(crate) struct DescribeClientQuotasRawTerminal {
    selected_version: Option<i16>,
    result: Result<DescribeClientQuotasResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeClientQuotasRawTerminal {
    pub(crate) fn fact(&self) -> DescribeClientQuotasTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeClientQuotasTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeClientQuotasTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Consumes terminal ownership after deterministic settlement.
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

pub(super) fn retain_describe_client_quotas_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeClientQuotasResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeClientQuotasRawTerminal {
    DescribeClientQuotasRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeClientQuotasDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeClientQuotasDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeClientQuotasDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeClientQuotasDriverFailureKind::Compatibility
        }
        _ => DescribeClientQuotasDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeClientQuotas ownership still requires core settlement"]
pub(crate) struct RecoveredDescribeClientQuotasCall {
    _private: (),
}

impl RecoveredDescribeClientQuotasCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
