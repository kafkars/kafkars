//! Neutral borrowed terminal facts for one exact-broker `DescribeLogDirs` call.

use kafka_client_core::{AdminDescribeLogDirsSelection, DeliveryStatus};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeLogDirsResponse;

use super::{
    super::request_failure_delivery, describe_log_dirs_call::evidence::DescribeLogDirsEvidence,
};

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeLogDirsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum DescribeLogDirsTerminalFact<'a> {
    Response {
        broker_id: i32,
        selected_version: Option<i16>,
        response: &'a DescribeLogDirsResponse,
    },
    Failed {
        kind: DescribeLogDirsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained through protocol validation and core settlement.
#[must_use = "a raw DescribeLogDirs terminal owns unsettled route evidence"]
pub(crate) struct DescribeLogDirsRawTerminal {
    evidence: DescribeLogDirsEvidence,
    selected_version: Option<i16>,
    result: Result<DescribeLogDirsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeLogDirsRawTerminal {
    pub(crate) fn matches(
        &self,
        broker_id: i32,
        selection: &AdminDescribeLogDirsSelection,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .matches(broker_id, selection, request_scratch_limit, result_limit)
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.evidence.result_limit()
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        broker_id: i32,
        selection: AdminDescribeLogDirsSelection,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        retain_describe_log_dirs_terminal(
            DescribeLogDirsEvidence::new(broker_id, selection, request_scratch_limit, result_limit),
            Some(ApiVersion::new(5)),
            Ok(DescribeLogDirsResponse::default()),
            None,
        )
    }

    pub(crate) fn fact(&self) -> DescribeLogDirsTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeLogDirsTerminalFact::Response {
                broker_id: self.evidence.broker_id(),
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeLogDirsTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Deliberately releases route evidence only after deterministic settlement.
    pub(crate) fn discard(self) {
        drop((self.result, self.route_token, self.evidence));
    }
}

pub(super) fn retain_describe_log_dirs_terminal(
    evidence: DescribeLogDirsEvidence,
    selected_version: Option<ApiVersion>,
    result: Result<DescribeLogDirsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeLogDirsRawTerminal {
    DescribeLogDirsRawTerminal {
        evidence,
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeLogDirsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeLogDirsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeLogDirsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeLogDirsDriverFailureKind::Compatibility
        }
        _ => DescribeLogDirsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeLogDirs ownership still requires settlement"]
pub(crate) struct RecoveredDescribeLogDirsCall {
    evidence: DescribeLogDirsEvidence,
}

impl RecoveredDescribeLogDirsCall {
    pub(super) const fn new(evidence: DescribeLogDirsEvidence) -> Self {
        Self { evidence }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        broker_id: i32,
        selection: AdminDescribeLogDirsSelection,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self::new(DescribeLogDirsEvidence::new(
            broker_id,
            selection,
            request_scratch_limit,
            result_limit,
        ))
    }

    pub(crate) fn matches(
        &self,
        broker_id: i32,
        selection: &AdminDescribeLogDirsSelection,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .matches(broker_id, selection, request_scratch_limit, result_limit)
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.evidence);
    }
}
