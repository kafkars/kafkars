//! Neutral borrowed terminal facts for one exact-broker `DescribeLogDirs` call.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeLogDirsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeLogDirsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum DescribeLogDirsTerminalFact<'a> {
    Response {
        broker_id: i32,
        selected_version: Option<i16>,
        response: &'a DescribeLogDirsResponse,
    },
    Failed {
        kind: DescribeLogDirsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained through protocol validation and core settlement.
#[must_use = "a raw DescribeLogDirs terminal owns unsettled route evidence"]
pub(crate) struct DescribeLogDirsRawTerminal {
    broker_id: i32,
    selected_version: Option<i16>,
    result: Result<DescribeLogDirsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeLogDirsRawTerminal {
    pub(crate) fn fact(&self) -> DescribeLogDirsTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeLogDirsTerminalFact::Response {
                broker_id: self.broker_id,
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeLogDirsTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Deliberately releases route evidence only after deterministic settlement.
    pub(crate) fn discard(self) {
        drop(self.route_token);
    }
}

pub(super) fn retain_describe_log_dirs_terminal(
    broker_id: i32,
    selected_version: Option<ApiVersion>,
    result: Result<DescribeLogDirsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeLogDirsRawTerminal {
    DescribeLogDirsRawTerminal {
        broker_id,
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeLogDirsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeLogDirsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeLogDirsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeLogDirsDriverFailureKind::Compatibility
        }
        _ => DescribeLogDirsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeLogDirs ownership still requires settlement"]
pub(crate) struct RecoveredDescribeLogDirsCall {
    _private: (),
}

impl RecoveredDescribeLogDirsCall {
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
