//! Neutral terminal facts for one tracked AnyBroker `DescribeClientQuotas` call.

use kafka_client_core::{DeliveryStatus, DescribeClientQuotasPlan};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeClientQuotasResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeClientQuotasDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum DescribeClientQuotasTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DescribeClientQuotasResponse,
    },
    Failed {
        kind: DescribeClientQuotasDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained through protocol validation and core settlement.
#[must_use = "a raw DescribeClientQuotas terminal must be deterministically settled"]
pub(crate) struct DescribeClientQuotasRawTerminal {
    selected_version: Option<i16>,
    result: Result<DescribeClientQuotasResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: DescribeClientQuotasPlan,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl DescribeClientQuotasRawTerminal {
    pub(crate) fn matches(
        &self,
        plan: &DescribeClientQuotasPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan
            && self.request_scratch_limit == request_scratch_limit
            && self.result_limit == result_limit
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        plan: DescribeClientQuotasPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        retain_describe_client_quotas_terminal(
            Some(ApiVersion::new(1)),
            Ok(DescribeClientQuotasResponse::default()),
            None,
            plan,
            request_scratch_limit,
            result_limit,
        )
    }

    pub(crate) fn fact(&self) -> DescribeClientQuotasTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeClientQuotasTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeClientQuotasTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Consumes terminal ownership after deterministic settlement.
    pub(crate) fn discard(self) {
        let Self {
            selected_version: _,
            result,
            route_token,
            plan,
            ..
        } = self;
        drop((result, route_token, plan));
    }
}

pub(super) fn retain_describe_client_quotas_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeClientQuotasResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: DescribeClientQuotasPlan,
    request_scratch_limit: usize,
    result_limit: usize,
) -> DescribeClientQuotasRawTerminal {
    DescribeClientQuotasRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        plan,
        request_scratch_limit,
        result_limit,
    }
}

fn failure_kind(error: &RequestError) -> DescribeClientQuotasDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeClientQuotasDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeClientQuotasDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeClientQuotasDriverFailureKind::Compatibility
        }
        _ => DescribeClientQuotasDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeClientQuotas ownership still requires core settlement"]
pub(crate) struct RecoveredDescribeClientQuotasCall {
    plan: DescribeClientQuotasPlan,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl RecoveredDescribeClientQuotasCall {
    pub(super) const fn new(
        plan: DescribeClientQuotasPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            plan,
            request_scratch_limit,
            result_limit,
        }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        plan: DescribeClientQuotasPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self::new(plan, request_scratch_limit, result_limit)
    }

    pub(crate) fn matches(
        &self,
        plan: &DescribeClientQuotasPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan
            && self.request_scratch_limit == request_scratch_limit
            && self.result_limit == result_limit
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.plan);
    }
}
