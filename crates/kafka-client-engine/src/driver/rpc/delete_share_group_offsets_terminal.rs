//! Neutral API-92 terminal facts with linear coordinator-route evidence.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DeleteShareGroupOffsetsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification independent of driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteShareGroupOffsetsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum DeleteShareGroupOffsetsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DeleteShareGroupOffsetsResponse,
    },
    Failed {
        kind: DeleteShareGroupOffsetsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained through normalization and core settlement.
#[must_use = "a raw API-92 terminal owns unsettled route evidence"]
pub(crate) struct DeleteShareGroupOffsetsTerminal {
    selected_version: Option<i16>,
    result: Result<DeleteShareGroupOffsetsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DeleteShareGroupOffsetsTerminal {
    pub(crate) fn fact(&self) -> DeleteShareGroupOffsetsTerminalFact<'_> {
        match &self.result {
            Ok(response) => DeleteShareGroupOffsetsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DeleteShareGroupOffsetsTerminalFact::Failed {
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

pub(super) fn retain_delete_share_group_offsets_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DeleteShareGroupOffsetsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DeleteShareGroupOffsetsTerminal {
    DeleteShareGroupOffsetsTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DeleteShareGroupOffsetsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DeleteShareGroupOffsetsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DeleteShareGroupOffsetsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DeleteShareGroupOffsetsDriverFailureKind::Compatibility
        }
        _ => DeleteShareGroupOffsetsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after unique driver destruction.
#[must_use = "recovered API-92 ownership still requires deterministic settlement"]
pub(crate) struct RecoveredDeleteShareGroupOffsetsCall {
    _private: (),
}

impl RecoveredDeleteShareGroupOffsetsCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
