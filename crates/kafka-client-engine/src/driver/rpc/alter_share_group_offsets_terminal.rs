//! Neutral API-91 terminal facts with linear coordinator-route evidence.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::AlterShareGroupOffsetsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification independent of driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterShareGroupOffsetsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum AlterShareGroupOffsetsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a AlterShareGroupOffsetsResponse,
    },
    Failed {
        kind: AlterShareGroupOffsetsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained through normalization and core settlement.
#[must_use = "a raw API-91 terminal owns unsettled route evidence"]
pub(crate) struct AlterShareGroupOffsetsTerminal {
    selected_version: Option<i16>,
    result: Result<AlterShareGroupOffsetsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl AlterShareGroupOffsetsTerminal {
    pub(crate) fn fact(&self) -> AlterShareGroupOffsetsTerminalFact<'_> {
        match &self.result {
            Ok(response) => AlterShareGroupOffsetsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => AlterShareGroupOffsetsTerminalFact::Failed {
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

pub(super) fn retain_alter_share_group_offsets_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<AlterShareGroupOffsetsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> AlterShareGroupOffsetsTerminal {
    AlterShareGroupOffsetsTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> AlterShareGroupOffsetsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => AlterShareGroupOffsetsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => AlterShareGroupOffsetsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            AlterShareGroupOffsetsDriverFailureKind::Compatibility
        }
        _ => AlterShareGroupOffsetsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after unique driver destruction.
#[must_use = "recovered API-91 ownership still requires deterministic settlement"]
pub(crate) struct RecoveredAlterShareGroupOffsetsCall {
    _private: (),
}

impl RecoveredAlterShareGroupOffsetsCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
