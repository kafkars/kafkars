//! Borrowed `OffsetCommit` terminal facts with linear route-receipt ownership.

use kafka_client_core::{AlterConsumerGroupOffsetsPlan, DeliveryStatus};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::OffsetCommitResponse;

use super::{super::request_failure_delivery, group_offset_alter_call::GroupOffsetAlterEvidence};

/// Stable engine-local classification without exposing driver error variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetAlterDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for a concrete host interpreter.
pub(crate) enum GroupOffsetAlterTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a OffsetCommitResponse,
    },
    Failed {
        kind: GroupOffsetAlterDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained until protocol validation and core settlement.
#[must_use = "a raw group-offset alteration terminal owns unsettled route evidence"]
pub(crate) struct GroupOffsetAlterTerminal {
    selected_version: Option<i16>,
    result: Result<OffsetCommitResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: GroupOffsetAlterEvidence,
}

impl GroupOffsetAlterTerminal {
    pub(crate) fn matches_evidence(
        &self,
        plan: &AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .matches(plan, request_scratch_limit, result_limit)
    }

    pub(crate) const fn response_plan(&self) -> &AlterConsumerGroupOffsetsPlan {
        self.evidence.plan()
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.evidence.result_limit()
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        plan: AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        retain_group_offset_alter_terminal(
            Some(ApiVersion::new(6)),
            Ok(OffsetCommitResponse::default()),
            None,
            GroupOffsetAlterEvidence::new(plan, request_scratch_limit, result_limit),
        )
    }

    pub(crate) fn fact(&self) -> GroupOffsetAlterTerminalFact<'_> {
        match &self.result {
            Ok(response) => GroupOffsetAlterTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => GroupOffsetAlterTerminalFact::Failed {
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

pub(super) fn retain_group_offset_alter_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<OffsetCommitResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: GroupOffsetAlterEvidence,
) -> GroupOffsetAlterTerminal {
    GroupOffsetAlterTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        evidence,
    }
}

fn failure_kind(error: &RequestError) -> GroupOffsetAlterDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => GroupOffsetAlterDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => GroupOffsetAlterDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            GroupOffsetAlterDriverFailureKind::Compatibility
        }
        _ => GroupOffsetAlterDriverFailureKind::Transport,
    }
}

/// Accepted call ownership recovered only after driver shutdown.
#[must_use = "recovered group-offset alteration ownership still requires core settlement"]
pub(crate) struct RecoveredGroupOffsetAlterCall {
    evidence: GroupOffsetAlterEvidence,
}

impl RecoveredGroupOffsetAlterCall {
    pub(super) const fn new(evidence: GroupOffsetAlterEvidence) -> Self {
        Self { evidence }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        plan: AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            evidence: GroupOffsetAlterEvidence::new(plan, request_scratch_limit, result_limit),
        }
    }

    pub(crate) fn matches_evidence(
        &self,
        plan: &AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .matches(plan, request_scratch_limit, result_limit)
    }

    /// Consumes recovered ownership after core receives the terminal fact.
    pub(crate) fn seal(self) {
        drop(self.evidence);
    }
}
