//! Neutral terminal facts for one tracked `AnyBroker` `DescribeAcls` call.

use kafka_client_core::{DeliveryStatus, DescribeAclsPlan};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeAclsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeAclsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum DescribeAclsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DescribeAclsResponse,
    },
    Failed {
        kind: DescribeAclsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained through protocol validation and core settlement.
#[must_use = "a raw DescribeAcls terminal must be deterministically settled"]
pub(crate) struct DescribeAclsRawTerminal {
    plan: DescribeAclsPlan,
    result_limit: usize,
    selected_version: Option<i16>,
    result: Result<DescribeAclsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeAclsRawTerminal {
    pub(crate) fn matches(&self, plan: &DescribeAclsPlan, result_limit: usize) -> bool {
        self.plan == *plan && self.result_limit == result_limit
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.result_limit
    }

    #[cfg(test)]
    pub(crate) fn for_test(plan: DescribeAclsPlan, result_limit: usize) -> Self {
        retain_describe_acls_terminal(
            plan,
            result_limit,
            Some(ApiVersion::new(1)),
            Ok(DescribeAclsResponse::default()),
            None,
        )
    }

    pub(crate) fn fact(&self) -> DescribeAclsTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeAclsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeAclsTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Consumes terminal ownership after deterministic settlement.
    pub(crate) fn discard(self) {
        let Self {
            plan,
            result,
            route_token,
            ..
        } = self;
        drop(plan);
        drop(result);
        drop(route_token);
    }
}

pub(super) fn retain_describe_acls_terminal(
    plan: DescribeAclsPlan,
    result_limit: usize,
    selected_version: Option<ApiVersion>,
    result: Result<DescribeAclsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeAclsRawTerminal {
    DescribeAclsRawTerminal {
        plan,
        result_limit,
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeAclsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeAclsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeAclsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => DescribeAclsDriverFailureKind::Compatibility,
        _ => DescribeAclsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeAcls ownership still requires core settlement"]
pub(crate) struct RecoveredDescribeAclsCall {
    plan: DescribeAclsPlan,
    result_limit: usize,
}

impl RecoveredDescribeAclsCall {
    pub(super) const fn new(plan: DescribeAclsPlan, result_limit: usize) -> Self {
        Self { plan, result_limit }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(plan: DescribeAclsPlan, result_limit: usize) -> Self {
        Self::new(plan, result_limit)
    }

    pub(crate) fn matches(&self, plan: &DescribeAclsPlan, result_limit: usize) -> bool {
        self.plan == *plan && self.result_limit == result_limit
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.plan);
    }
}
