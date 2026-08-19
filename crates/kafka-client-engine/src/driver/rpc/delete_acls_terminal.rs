//! Neutral terminal facts for one tracked `AnyBroker` `DeleteAcls` call.

use kafka_client_core::{DeleteAclsPlan, DeliveryStatus};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DeleteAclsResponse;

use super::{super::request_failure_delivery, delete_acls_call::DeleteAclsEvidence};

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteAclsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure safe for deterministic host settlement.
pub(crate) enum DeleteAclsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DeleteAclsResponse,
    },
    Failed {
        kind: DeleteAclsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response and tracked route evidence retained through core settlement.
#[must_use = "a raw DeleteAcls terminal must be deterministically settled"]
pub(crate) struct DeleteAclsRawTerminal {
    selected_version: Option<i16>,
    result: Result<DeleteAclsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: DeleteAclsEvidence,
}

impl DeleteAclsRawTerminal {
    pub(crate) fn fact(&self) -> DeleteAclsTerminalFact<'_> {
        match &self.result {
            Ok(response) => DeleteAclsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DeleteAclsTerminalFact::Failed {
                kind: delete_acls_failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    pub(crate) const fn plan(&self) -> &DeleteAclsPlan {
        self.evidence.plan()
    }

    pub(crate) const fn request_limit(&self) -> usize {
        self.evidence.request_limit()
    }

    pub(crate) fn matches(
        &self,
        plan: &DeleteAclsPlan,
        request_limit: usize,
        nested_count_capacity: usize,
        result_capacity: usize,
        outcome_capacity: usize,
    ) -> bool {
        self.evidence.matches(
            plan,
            request_limit,
            nested_count_capacity,
            result_capacity,
            outcome_capacity,
        )
    }

    /// Deliberately releases response and route evidence after core settlement.
    pub(crate) fn discard(self) {
        let Self {
            selected_version: _,
            result,
            route_token,
            evidence,
        } = self;
        drop(result);
        drop(route_token);
        drop(evidence);
    }
}

pub(super) fn retain_delete_acls_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DeleteAclsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: DeleteAclsEvidence,
) -> DeleteAclsRawTerminal {
    DeleteAclsRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        evidence,
    }
}

pub(super) fn delete_acls_failure_kind(error: &RequestError) -> DeleteAclsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DeleteAclsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DeleteAclsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => DeleteAclsDriverFailureKind::Compatibility,
        RequestError::ResponseCapacityReached { .. }
        | RequestError::IdentityConflict
        | RequestError::DeadlineOverflow
        | RequestError::RouteUnavailable
        | RequestError::RouteCapacityReached { .. }
        | RequestError::MetadataQueryCapacityReached { .. }
        | RequestError::CoordinatorCapacityReached { .. }
        | RequestError::NameResolutionCapacityReached { .. }
        | RequestError::NameResolutionFailed { .. }
        | RequestError::Rejected { .. }
        | RequestError::ConnectionClosed(_) => DeleteAclsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DeleteAcls ownership still requires core settlement"]
pub(crate) struct RecoveredDeleteAclsCall {
    evidence: DeleteAclsEvidence,
}

impl RecoveredDeleteAclsCall {
    pub(super) const fn new(evidence: DeleteAclsEvidence) -> Self {
        Self { evidence }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        plan: DeleteAclsPlan,
        request_limit: usize,
        nested_count_capacity: usize,
        result_capacity: usize,
        outcome_capacity: usize,
    ) -> Self {
        Self {
            evidence: DeleteAclsEvidence::new(
                plan,
                request_limit,
                nested_count_capacity,
                result_capacity,
                outcome_capacity,
            ),
        }
    }

    pub(crate) fn matches(
        &self,
        plan: &DeleteAclsPlan,
        request_limit: usize,
        nested_count_capacity: usize,
        result_capacity: usize,
        outcome_capacity: usize,
    ) -> bool {
        self.evidence.matches(
            plan,
            request_limit,
            nested_count_capacity,
            result_capacity,
            outcome_capacity,
        )
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.evidence);
    }
}
