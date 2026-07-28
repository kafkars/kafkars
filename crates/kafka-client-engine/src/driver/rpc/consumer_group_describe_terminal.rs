//! Borrowed API-key 69 terminal facts with explicit fallback and route ownership.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::ConsumerGroupDescribeResponse;
use kafka_wire_core::EncodeError;

use super::super::request_failure_delivery;

/// Stable failure classification independent of driver error variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupDescribeDriverFailureKind {
    DeadlineElapsed,
    LocalApiUnavailable,
    LocalUnsupportedVersion,
    InvalidResponse,
    Transport,
}

/// Borrowed terminal safe for the future modern-first host interpreter.
pub(crate) enum ConsumerGroupDescribeTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a ConsumerGroupDescribeResponse,
    },
    Failed {
        kind: ConsumerGroupDescribeDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained through normalization and deterministic settlement.
#[must_use = "a raw ConsumerGroupDescribe terminal owns unsettled route evidence"]
pub(crate) struct ConsumerGroupDescribeRawTerminal {
    selected_version: Option<i16>,
    result: Result<ConsumerGroupDescribeResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl ConsumerGroupDescribeRawTerminal {
    pub(crate) fn fact(&self) -> ConsumerGroupDescribeTerminalFact<'_> {
        match &self.result {
            Ok(response) => ConsumerGroupDescribeTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => ConsumerGroupDescribeTerminalFact::Failed {
                kind: consumer_group_describe_failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Deliberately releases coordinator evidence after deterministic settlement.
    pub(crate) fn discard(self) {
        drop(self.route_token);
    }
}

pub(super) fn retain_consumer_group_describe_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<ConsumerGroupDescribeResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ConsumerGroupDescribeRawTerminal {
    ConsumerGroupDescribeRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

pub(super) fn consumer_group_describe_failure_kind(
    error: &RequestError,
) -> ConsumerGroupDescribeDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => ConsumerGroupDescribeDriverFailureKind::DeadlineElapsed,
        RequestError::ApiUnavailable { .. } => {
            ConsumerGroupDescribeDriverFailureKind::LocalApiUnavailable
        }
        RequestError::Encode(EncodeError::UnsupportedVersion { .. })
        | RequestError::UnsupportedVersion { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            ConsumerGroupDescribeDriverFailureKind::LocalUnsupportedVersion
        }
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_)
        | RequestError::Encode(_) => ConsumerGroupDescribeDriverFailureKind::InvalidResponse,
        _ => ConsumerGroupDescribeDriverFailureKind::Transport,
    }
}

/// Accepted call ownership recovered only after unique-driver shutdown.
#[must_use = "recovered ConsumerGroupDescribe ownership still requires settlement"]
pub(crate) struct RecoveredConsumerGroupDescribeCall {
    _private: (),
}

impl RecoveredConsumerGroupDescribeCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
