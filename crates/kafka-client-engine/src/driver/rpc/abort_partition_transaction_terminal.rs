//! Neutral borrowed terminal facts for one API27 leader call.

use kafka_client_core::{AbortPartitionTransactionPlan, DeliveryStatus};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::WriteTxnMarkersResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AbortPartitionTransactionDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed terminal fact safe for the concrete operation host.
pub(crate) enum AbortPartitionTransactionTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a WriteTxnMarkersResponse,
    },
    Failed {
        kind: AbortPartitionTransactionDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response and route evidence retained through deterministic settlement.
#[must_use = "a raw partition transaction-abort terminal owns route evidence"]
pub(crate) struct AbortPartitionTransactionRawTerminal {
    selected_version: Option<i16>,
    result: Result<WriteTxnMarkersResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: AbortPartitionTransactionPlan,
}

impl AbortPartitionTransactionRawTerminal {
    pub(crate) fn fact(&self) -> AbortPartitionTransactionTerminalFact<'_> {
        match &self.result {
            Ok(response) => AbortPartitionTransactionTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => AbortPartitionTransactionTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn matches_plan_for_test(&self, expected: &AbortPartitionTransactionPlan) -> bool {
        &self.plan == expected
    }

    /// Deliberately releases route evidence after core accepts the terminal fact.
    pub(crate) fn discard(self) {
        let Self {
            selected_version: _,
            result,
            route_token,
            plan,
        } = self;
        drop(result);
        drop(route_token);
        drop(plan);
    }
}

pub(super) fn retain_abort_partition_transaction_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<WriteTxnMarkersResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: AbortPartitionTransactionPlan,
) -> AbortPartitionTransactionRawTerminal {
    AbortPartitionTransactionRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        plan,
    }
}

fn failure_kind(error: &RequestError) -> AbortPartitionTransactionDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => AbortPartitionTransactionDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => AbortPartitionTransactionDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            AbortPartitionTransactionDriverFailureKind::Compatibility
        }
        _ => AbortPartitionTransactionDriverFailureKind::Transport,
    }
}

/// Accepted call ownership recovered only after driver shutdown.
#[must_use = "recovered partition transaction-abort ownership requires settlement"]
pub(crate) struct RecoveredAbortPartitionTransactionCall {
    plan: AbortPartitionTransactionPlan,
}

impl RecoveredAbortPartitionTransactionCall {
    pub(super) const fn new(plan: AbortPartitionTransactionPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(plan: AbortPartitionTransactionPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) fn matches_plan_for_test(&self, expected: &AbortPartitionTransactionPlan) -> bool {
        &self.plan == expected
    }

    /// Consumes the recovered proof after conservative core settlement.
    pub(crate) fn seal(self) {
        drop(self.plan);
    }
}
