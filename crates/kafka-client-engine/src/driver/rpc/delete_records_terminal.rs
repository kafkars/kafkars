//! Neutral borrowed terminal facts for one Admin `DeleteRecords` leader call.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DeleteRecordsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteRecordsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host.
pub(crate) enum DeleteRecordsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DeleteRecordsResponse,
    },
    Failed {
        kind: DeleteRecordsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained until protocol validation and core settlement finish.
#[must_use = "a raw Admin DeleteRecords terminal owns unsettled route evidence"]
pub(crate) struct DeleteRecordsRawTerminal {
    selected_version: Option<i16>,
    result: Result<DeleteRecordsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DeleteRecordsRawTerminal {
    pub(crate) fn fact(&self) -> DeleteRecordsTerminalFact<'_> {
        match &self.result {
            Ok(response) => DeleteRecordsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DeleteRecordsTerminalFact::Failed {
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

pub(super) fn retain_delete_records_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DeleteRecordsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DeleteRecordsRawTerminal {
    DeleteRecordsRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DeleteRecordsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DeleteRecordsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DeleteRecordsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DeleteRecordsDriverFailureKind::Compatibility
        }
        _ => DeleteRecordsDriverFailureKind::Transport,
    }
}

/// Accepted call ownership recovered only after driver shutdown.
#[must_use = "recovered Admin DeleteRecords ownership still requires core settlement"]
pub(crate) struct RecoveredDeleteRecordsCall {
    _private: (),
}

impl RecoveredDeleteRecordsCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    #[cfg(test)]
    pub(crate) const fn for_test() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered call ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
