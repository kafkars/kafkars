//! Neutral borrowed terminal facts for one Admin `DeleteConsumerGroups` coordinator call.

use kafka_client_core::{DeleteConsumerGroupsPlan, DeleteConsumerGroupsTarget, DeliveryStatus};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DeleteGroupsResponse;

use super::{
    super::request_failure_delivery, delete_consumer_groups_call::DeleteConsumerGroupsEvidence,
};

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteConsumerGroupsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host.
pub(crate) enum DeleteConsumerGroupsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DeleteGroupsResponse,
    },
    Failed {
        kind: DeleteConsumerGroupsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained until protocol validation and core settlement finish.
#[must_use = "a raw Admin DeleteConsumerGroups terminal owns unsettled route evidence"]
pub(crate) struct DeleteConsumerGroupsRawTerminal {
    selected_version: Option<i16>,
    result: Result<DeleteGroupsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: DeleteConsumerGroupsEvidence,
}

impl DeleteConsumerGroupsRawTerminal {
    #[cfg(test)]
    pub(crate) fn for_test(
        plan: DeleteConsumerGroupsPlan,
        target: DeleteConsumerGroupsTarget,
        request_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            selected_version: Some(3),
            result: Ok(DeleteGroupsResponse::default()),
            route_token: None,
            evidence: DeleteConsumerGroupsEvidence::new(plan, target, request_limit, result_limit),
        }
    }

    pub(crate) fn matches_evidence(
        &self,
        plan: &DeleteConsumerGroupsPlan,
        target: &DeleteConsumerGroupsTarget,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .matches(plan, target, request_limit, result_limit)
    }

    pub(crate) const fn response_target(&self) -> &DeleteConsumerGroupsTarget {
        self.evidence.target()
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.evidence.result_limit()
    }

    pub(crate) fn fact(&self) -> DeleteConsumerGroupsTerminalFact<'_> {
        match &self.result {
            Ok(response) => DeleteConsumerGroupsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DeleteConsumerGroupsTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Deliberately releases route evidence only after deterministic settlement.
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

pub(super) fn retain_delete_consumer_groups_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DeleteGroupsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: DeleteConsumerGroupsEvidence,
) -> DeleteConsumerGroupsRawTerminal {
    DeleteConsumerGroupsRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        evidence,
    }
}

fn failure_kind(error: &RequestError) -> DeleteConsumerGroupsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DeleteConsumerGroupsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DeleteConsumerGroupsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DeleteConsumerGroupsDriverFailureKind::Compatibility
        }
        _ => DeleteConsumerGroupsDriverFailureKind::Transport,
    }
}

/// Accepted call ownership recovered only after driver shutdown.
#[must_use = "recovered Admin DeleteConsumerGroups ownership still requires core settlement"]
pub(crate) struct RecoveredDeleteConsumerGroupsCall {
    evidence: DeleteConsumerGroupsEvidence,
}

impl RecoveredDeleteConsumerGroupsCall {
    pub(super) const fn new(evidence: DeleteConsumerGroupsEvidence) -> Self {
        Self { evidence }
    }

    pub(crate) fn matches_evidence(
        &self,
        plan: &DeleteConsumerGroupsPlan,
        target: &DeleteConsumerGroupsTarget,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .matches(plan, target, request_limit, result_limit)
    }

    /// Consumes recovered call ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.evidence);
    }
}
