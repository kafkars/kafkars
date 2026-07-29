//! Neutral terminal facts for one tracked transaction-coordinator description call.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeTransactionsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeTransactionsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum DescribeTransactionsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DescribeTransactionsResponse,
    },
    Failed {
        kind: DescribeTransactionsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw DescribeTransactions terminal must be deterministically settled"]
pub(crate) struct DescribeTransactionsRawTerminal {
    selected_version: Option<i16>,
    result: Result<DescribeTransactionsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeTransactionsRawTerminal {
    pub(crate) fn fact(&self) -> DescribeTransactionsTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeTransactionsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeTransactionsTerminalFact::Failed {
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

pub(super) fn retain_describe_transactions_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeTransactionsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeTransactionsRawTerminal {
    DescribeTransactionsRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeTransactionsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeTransactionsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeTransactionsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeTransactionsDriverFailureKind::Compatibility
        }
        _ => DescribeTransactionsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeTransactions ownership still requires core settlement"]
pub(crate) struct RecoveredDescribeTransactionsCall;

impl RecoveredDescribeTransactionsCall {
    /// Consumes recovered ownership after deterministic settlement.
    pub(crate) const fn seal(self) {
        let Self = self;
    }
}
