//! Neutral terminal facts for one tracked metadata-quorum description call.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeQuorumResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeMetadataQuorumDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum DescribeMetadataQuorumTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DescribeQuorumResponse,
    },
    Failed {
        kind: DescribeMetadataQuorumDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw DescribeMetadataQuorum terminal must be deterministically settled"]
pub(crate) struct DescribeMetadataQuorumRawTerminal {
    selected_version: Option<i16>,
    result: Result<DescribeQuorumResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeMetadataQuorumRawTerminal {
    pub(crate) fn fact(&self) -> DescribeMetadataQuorumTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeMetadataQuorumTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeMetadataQuorumTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Consumes the driver-owned response only after deterministic settlement.
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

pub(super) fn retain_describe_metadata_quorum_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeQuorumResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeMetadataQuorumRawTerminal {
    DescribeMetadataQuorumRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeMetadataQuorumDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeMetadataQuorumDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeMetadataQuorumDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeMetadataQuorumDriverFailureKind::Compatibility
        }
        _ => DescribeMetadataQuorumDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeMetadataQuorum ownership still requires core settlement"]
pub(crate) struct RecoveredDescribeMetadataQuorumCall {
    _private: (),
}

impl RecoveredDescribeMetadataQuorumCall {
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
