//! Neutral terminal facts for one tracked `AnyBroker` `CreateAcls` call.

use kafka_client_core::{CreateAclsPlan, DeliveryStatus};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::CreateAclsResponse;

use super::{super::request_failure_delivery, create_acls_call::CreateAclsEvidence};

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateAclsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure safe for deterministic host settlement.
pub(crate) enum CreateAclsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a CreateAclsResponse,
    },
    Failed {
        kind: CreateAclsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response and tracked route evidence retained through core settlement.
#[must_use = "a raw CreateAcls terminal must be deterministically settled"]
pub(crate) struct CreateAclsRawTerminal {
    selected_version: Option<i16>,
    result: Result<CreateAclsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: CreateAclsEvidence,
}

impl CreateAclsRawTerminal {
    pub(crate) fn matches_evidence(
        &self,
        plan: &CreateAclsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence.matches(plan, request_limit, result_limit)
    }

    pub(crate) const fn response_plan(&self) -> &CreateAclsPlan {
        self.evidence.plan()
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.evidence.result_limit()
    }

    pub(crate) fn fact(&self) -> CreateAclsTerminalFact<'_> {
        match &self.result {
            Ok(response) => CreateAclsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => CreateAclsTerminalFact::Failed {
                kind: create_acls_failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Deliberately releases response and route evidence after core settlement.
    pub(crate) fn discard(self) {
        let Self {
            selected_version: _,
            result,
            route_token,
            evidence,
        } = self;
        drop((result, route_token, evidence));
    }
}

pub(super) fn retain_create_acls_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<CreateAclsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: CreateAclsEvidence,
) -> CreateAclsRawTerminal {
    CreateAclsRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        evidence,
    }
}

pub(super) fn create_acls_failure_kind(error: &RequestError) -> CreateAclsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => CreateAclsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => CreateAclsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => CreateAclsDriverFailureKind::Compatibility,
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
        | RequestError::ConnectionClosed(_) => CreateAclsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered CreateAcls ownership still requires core settlement"]
pub(crate) struct RecoveredCreateAclsCall {
    evidence: CreateAclsEvidence,
}

impl RecoveredCreateAclsCall {
    pub(super) const fn new(evidence: CreateAclsEvidence) -> Self {
        Self { evidence }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        plan: CreateAclsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self::new(CreateAclsEvidence::new(plan, request_limit, result_limit))
    }

    pub(crate) fn matches_evidence(
        &self,
        plan: &CreateAclsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence.matches(plan, request_limit, result_limit)
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.evidence);
    }
}
