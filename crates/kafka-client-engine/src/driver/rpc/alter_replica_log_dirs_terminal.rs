//! Neutral borrowed terminal facts for one exact-broker `AlterReplicaLogDirs` call.

use kafka_client_core::{AlterReplicaLogDirAssignment, DeliveryStatus};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::AlterReplicaLogDirsResponse;

use super::{
    super::request_failure_delivery, alter_replica_log_dirs_call::AlterReplicaLogDirsEvidence,
};

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterReplicaLogDirsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum AlterReplicaLogDirsTerminalFact<'a> {
    Response {
        broker_id: i32,
        selected_version: Option<i16>,
        response: &'a AlterReplicaLogDirsResponse,
    },
    Failed {
        kind: AlterReplicaLogDirsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained through protocol validation and core settlement.
#[must_use = "a raw AlterReplicaLogDirs terminal owns unsettled route evidence"]
pub(crate) struct AlterReplicaLogDirsRawTerminal {
    selected_version: Option<i16>,
    result: Result<AlterReplicaLogDirsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: AlterReplicaLogDirsEvidence,
}

impl AlterReplicaLogDirsRawTerminal {
    #[cfg(test)]
    pub(crate) fn for_test(
        broker_id: i32,
        assignments: Vec<AlterReplicaLogDirAssignment>,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            selected_version: Some(1),
            result: Ok(AlterReplicaLogDirsResponse::default()),
            route_token: None,
            evidence: AlterReplicaLogDirsEvidence::new(
                broker_id,
                assignments,
                request_scratch_limit,
                result_limit,
            ),
        }
    }

    pub(crate) fn fact(&self) -> AlterReplicaLogDirsTerminalFact<'_> {
        match &self.result {
            Ok(response) => AlterReplicaLogDirsTerminalFact::Response {
                broker_id: self.evidence.broker_id(),
                selected_version: self.selected_version,
                response,
            },
            Err(error) => AlterReplicaLogDirsTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    pub(crate) fn assignments(&self) -> &[AlterReplicaLogDirAssignment] {
        self.evidence.assignments()
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.evidence.result_limit()
    }

    pub(crate) fn matches_evidence(
        &self,
        broker_id: i32,
        assignments: &[AlterReplicaLogDirAssignment],
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .matches(broker_id, assignments, request_scratch_limit, result_limit)
    }

    /// Deliberately releases route evidence only after deterministic settlement.
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

pub(super) fn retain_alter_replica_log_dirs_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<AlterReplicaLogDirsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: AlterReplicaLogDirsEvidence,
) -> AlterReplicaLogDirsRawTerminal {
    AlterReplicaLogDirsRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        evidence,
    }
}

fn failure_kind(error: &RequestError) -> AlterReplicaLogDirsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => AlterReplicaLogDirsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => AlterReplicaLogDirsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            AlterReplicaLogDirsDriverFailureKind::Compatibility
        }
        _ => AlterReplicaLogDirsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered AlterReplicaLogDirs ownership still requires settlement"]
pub(crate) struct RecoveredAlterReplicaLogDirsCall {
    evidence: AlterReplicaLogDirsEvidence,
}

impl RecoveredAlterReplicaLogDirsCall {
    pub(super) const fn new(evidence: AlterReplicaLogDirsEvidence) -> Self {
        Self { evidence }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        broker_id: i32,
        assignments: Vec<AlterReplicaLogDirAssignment>,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            evidence: AlterReplicaLogDirsEvidence::new(
                broker_id,
                assignments,
                request_scratch_limit,
                result_limit,
            ),
        }
    }

    pub(crate) fn matches_evidence(
        &self,
        broker_id: i32,
        assignments: &[AlterReplicaLogDirAssignment],
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .matches(broker_id, assignments, request_scratch_limit, result_limit)
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.evidence);
    }
}
