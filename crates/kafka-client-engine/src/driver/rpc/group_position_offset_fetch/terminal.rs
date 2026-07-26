//! Raw versioned `OffsetFetch` terminal retained for the execution owner.

use kafka_driver::{ApiVersion, CallFailure, RequestError};
use kafka_wire::OffsetFetchResponse;

use super::key::GroupPositionOffsetFetchKey;

/// Stable engine-local classification without exposing driver error variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupPositionOffsetFetchDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or closed driver failure fact for consumer interpretation.
#[derive(Clone, Copy)]
pub(crate) enum GroupPositionOffsetFetchTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a OffsetFetchResponse,
    },
    Failed {
        kind: GroupPositionOffsetFetchDriverFailureKind,
    },
}

/// Uninterpreted response or driver-authoritative request failure.
#[must_use = "a raw group position terminal owns an unsettled assignment request"]
pub(crate) struct GroupPositionOffsetFetchTerminal {
    key: GroupPositionOffsetFetchKey,
    selected_version: Option<i16>,
    result: Result<OffsetFetchResponse, RequestError>,
}

impl GroupPositionOffsetFetchTerminal {
    pub(crate) const fn key(&self) -> &GroupPositionOffsetFetchKey {
        &self.key
    }

    pub(crate) fn fact(&self) -> GroupPositionOffsetFetchTerminalFact<'_> {
        match &self.result {
            Ok(response) => GroupPositionOffsetFetchTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => GroupPositionOffsetFetchTerminalFact::Failed {
                kind: failure_kind(error),
            },
        }
    }
}

pub(super) fn retain_group_position_offset_fetch_terminal(
    key: GroupPositionOffsetFetchKey,
    selected_version: Option<ApiVersion>,
    result: Result<OffsetFetchResponse, RequestError>,
) -> GroupPositionOffsetFetchTerminal {
    GroupPositionOffsetFetchTerminal {
        key,
        selected_version: selected_version.map(ApiVersion::value),
        result,
    }
}

fn failure_kind(error: &RequestError) -> GroupPositionOffsetFetchDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => GroupPositionOffsetFetchDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => GroupPositionOffsetFetchDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            GroupPositionOffsetFetchDriverFailureKind::Compatibility
        }
        _ => GroupPositionOffsetFetchDriverFailureKind::Transport,
    }
}
