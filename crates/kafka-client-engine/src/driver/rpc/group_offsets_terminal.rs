//! Neutral borrowed `OffsetFetch` terminal facts with opaque route ownership.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::OffsetFetchResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver error variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for a concrete host interpreter.
pub(crate) enum GroupOffsetsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a OffsetFetchResponse,
    },
    Failed {
        kind: GroupOffsetsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained until borrowed protocol validation completes.
#[must_use = "a raw group-offset terminal owns unsettled route evidence"]
pub(crate) struct GroupOffsetsTerminal {
    selected_version: Option<i16>,
    result: Result<OffsetFetchResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl GroupOffsetsTerminal {
    pub(crate) fn fact(&self) -> GroupOffsetsTerminalFact<'_> {
        match &self.result {
            Ok(response) => GroupOffsetsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => GroupOffsetsTerminalFact::Failed {
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

pub(super) fn retain_group_offsets_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<OffsetFetchResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> GroupOffsetsTerminal {
    GroupOffsetsTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> GroupOffsetsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => GroupOffsetsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => GroupOffsetsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => GroupOffsetsDriverFailureKind::Compatibility,
        _ => GroupOffsetsDriverFailureKind::Transport,
    }
}

/// Accepted call ownership recovered only after driver shutdown.
#[must_use = "recovered group-offset ownership still requires core settlement"]
pub(crate) struct RecoveredGroupOffsetsCall {
    _private: (),
}

impl RecoveredGroupOffsetsCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered call ownership after the core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
