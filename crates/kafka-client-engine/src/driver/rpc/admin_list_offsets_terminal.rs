//! Neutral borrowed terminal facts for one Admin `ListOffsets` leader call.

use kafka_client_core::{AdminListOffsetTarget, DeliveryStatus, ReadIsolation};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::ListOffsetsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminListOffsetsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host.
pub(crate) enum AdminListOffsetsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a ListOffsetsResponse,
    },
    Failed {
        kind: AdminListOffsetsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained until protocol validation and core settlement finish.
#[must_use = "a raw Admin ListOffsets terminal owns unsettled route evidence"]
pub(crate) struct AdminListOffsetsTerminal {
    selected_version: Option<i16>,
    result: Result<ListOffsetsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    target: AdminListOffsetTarget,
    read_isolation: ReadIsolation,
}

impl AdminListOffsetsTerminal {
    pub(crate) fn fact(&self) -> AdminListOffsetsTerminalFact<'_> {
        match &self.result {
            Ok(response) => AdminListOffsetsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => AdminListOffsetsTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    pub(crate) fn matches_correlation(
        &self,
        expected_target: &AdminListOffsetTarget,
        expected_read_isolation: ReadIsolation,
    ) -> bool {
        &self.target == expected_target && self.read_isolation == expected_read_isolation
    }

    /// Deliberately releases route evidence only after deterministic settlement.
    pub(crate) fn discard(self) {
        let Self {
            selected_version: _,
            result,
            route_token,
            target,
            read_isolation: _,
        } = self;
        drop(result);
        drop(route_token);
        drop(target);
    }
}

pub(super) fn retain_admin_list_offsets_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<ListOffsetsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    target: AdminListOffsetTarget,
    read_isolation: ReadIsolation,
) -> AdminListOffsetsTerminal {
    AdminListOffsetsTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        target,
        read_isolation,
    }
}

fn failure_kind(error: &RequestError) -> AdminListOffsetsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => AdminListOffsetsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => AdminListOffsetsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            AdminListOffsetsDriverFailureKind::Compatibility
        }
        _ => AdminListOffsetsDriverFailureKind::Transport,
    }
}

/// Accepted call ownership recovered only after driver shutdown.
#[must_use = "recovered Admin ListOffsets ownership still requires core settlement"]
pub(crate) struct RecoveredAdminListOffsetsCall {
    target: AdminListOffsetTarget,
    read_isolation: ReadIsolation,
}

impl RecoveredAdminListOffsetsCall {
    pub(super) const fn new(target: AdminListOffsetTarget, read_isolation: ReadIsolation) -> Self {
        Self {
            target,
            read_isolation,
        }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        target: AdminListOffsetTarget,
        read_isolation: ReadIsolation,
    ) -> Self {
        Self::new(target, read_isolation)
    }

    pub(crate) fn matches_correlation(
        &self,
        expected_target: &AdminListOffsetTarget,
        expected_read_isolation: ReadIsolation,
    ) -> bool {
        &self.target == expected_target && self.read_isolation == expected_read_isolation
    }

    /// Consumes recovered call ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        let Self {
            target,
            read_isolation: _,
        } = self;
        drop(target);
    }
}
