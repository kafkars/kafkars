//! Borrowed `OffsetDelete` terminal facts with linear route-receipt ownership.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::OffsetDeleteResponse;

use super::{super::request_failure_delivery, group_offset_delete_call::GroupOffsetDeleteEvidence};

/// Stable engine-local classification without exposing driver error variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetDeleteDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for a concrete host interpreter.
pub(crate) enum GroupOffsetDeleteTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a OffsetDeleteResponse,
    },
    Failed {
        kind: GroupOffsetDeleteDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained until protocol validation and core settlement.
#[must_use = "a raw group-offset deletion terminal owns unsettled route evidence"]
pub(crate) struct GroupOffsetDeleteTerminal {
    selected_version: Option<i16>,
    result: Result<OffsetDeleteResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: GroupOffsetDeleteEvidence,
}

impl GroupOffsetDeleteTerminal {
    pub(crate) fn matches_evidence(
        &self,
        plan: &kafka_client_core::DeleteConsumerGroupOffsetsPlan,
        result_limit: usize,
    ) -> bool {
        self.evidence.matches(plan, result_limit)
    }

    pub(crate) const fn response_plan(&self) -> &kafka_client_core::DeleteConsumerGroupOffsetsPlan {
        self.evidence.plan()
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.evidence.result_limit()
    }

    pub(crate) fn fact(&self) -> GroupOffsetDeleteTerminalFact<'_> {
        match &self.result {
            Ok(response) => GroupOffsetDeleteTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => GroupOffsetDeleteTerminalFact::Failed {
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

pub(super) fn retain_group_offset_delete_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<OffsetDeleteResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: GroupOffsetDeleteEvidence,
) -> GroupOffsetDeleteTerminal {
    GroupOffsetDeleteTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        evidence,
    }
}

fn failure_kind(error: &RequestError) -> GroupOffsetDeleteDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => GroupOffsetDeleteDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => GroupOffsetDeleteDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            GroupOffsetDeleteDriverFailureKind::Compatibility
        }
        _ => GroupOffsetDeleteDriverFailureKind::Transport,
    }
}

/// Accepted call ownership recovered only after driver shutdown.
#[must_use = "recovered group-offset deletion ownership still requires core settlement"]
pub(crate) struct RecoveredGroupOffsetDeleteCall {
    evidence: GroupOffsetDeleteEvidence,
}

impl RecoveredGroupOffsetDeleteCall {
    pub(super) const fn new(evidence: GroupOffsetDeleteEvidence) -> Self {
        Self { evidence }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        plan: kafka_client_core::DeleteConsumerGroupOffsetsPlan,
        result_limit: usize,
    ) -> Self {
        Self {
            evidence: GroupOffsetDeleteEvidence::new(plan, result_limit),
        }
    }

    pub(crate) fn matches_evidence(
        &self,
        plan: &kafka_client_core::DeleteConsumerGroupOffsetsPlan,
        result_limit: usize,
    ) -> bool {
        self.evidence.matches(plan, result_limit)
    }

    /// Consumes recovered ownership after core receives the terminal fact.
    pub(crate) fn seal(self) {
        drop(self.evidence);
    }
}
