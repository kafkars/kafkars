//! Neutral terminal facts for one tracked topic-partition description call.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeTopicPartitionsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeTopicPartitionsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum DescribeTopicPartitionsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DescribeTopicPartitionsResponse,
    },
    Failed {
        kind: DescribeTopicPartitionsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw DescribeTopicPartitions terminal must be deterministically settled"]
pub(crate) struct DescribeTopicPartitionsRawTerminal {
    selected_version: Option<i16>,
    result: Result<DescribeTopicPartitionsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeTopicPartitionsRawTerminal {
    pub(crate) fn fact(&self) -> DescribeTopicPartitionsTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeTopicPartitionsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeTopicPartitionsTerminalFact::Failed {
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

pub(super) fn retain_describe_topic_partitions_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeTopicPartitionsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeTopicPartitionsRawTerminal {
    DescribeTopicPartitionsRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeTopicPartitionsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeTopicPartitionsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeTopicPartitionsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeTopicPartitionsDriverFailureKind::Compatibility
        }
        _ => DescribeTopicPartitionsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeTopicPartitions ownership still requires core settlement"]
pub(crate) struct RecoveredDescribeTopicPartitionsCall {
    _private: (),
}

impl RecoveredDescribeTopicPartitionsCall {
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
