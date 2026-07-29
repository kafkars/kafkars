//! Neutral terminal facts for one tracked delegation-token expiration.

use kafka_client_core::{DeliveryStatus, ExpireDelegationTokenPlan};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::ExpireDelegationTokenResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpireDelegationTokenDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or failure for the deterministic host interpreter.
pub(crate) enum ExpireDelegationTokenTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a ExpireDelegationTokenResponse,
    },
    Failed {
        kind: ExpireDelegationTokenDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response and route evidence retained through deterministic settlement.
#[must_use = "a raw ExpireDelegationToken terminal must be deterministically settled"]
pub(crate) struct ExpireDelegationTokenRawTerminal {
    selected_version: Option<i16>,
    result: Result<ExpireDelegationTokenResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: ExpireDelegationTokenPlan,
}

impl ExpireDelegationTokenRawTerminal {
    pub(crate) fn fact(&self) -> ExpireDelegationTokenTerminalFact<'_> {
        match &self.result {
            Ok(response) => ExpireDelegationTokenTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => ExpireDelegationTokenTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Releases response and route evidence only after core settlement.
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

pub(super) fn retain_expire_delegation_token_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<ExpireDelegationTokenResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: ExpireDelegationTokenPlan,
) -> ExpireDelegationTokenRawTerminal {
    ExpireDelegationTokenRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        plan,
    }
}

fn failure_kind(error: &RequestError) -> ExpireDelegationTokenDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => ExpireDelegationTokenDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => ExpireDelegationTokenDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            ExpireDelegationTokenDriverFailureKind::Compatibility
        }
        _ => ExpireDelegationTokenDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered ExpireDelegationToken ownership still requires core settlement"]
pub(crate) struct RecoveredExpireDelegationTokenCall {
    plan: ExpireDelegationTokenPlan,
}

impl RecoveredExpireDelegationTokenCall {
    pub(super) const fn new(plan: ExpireDelegationTokenPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(plan: ExpireDelegationTokenPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) fn matches_correlation_for_test(
        &self,
        expected_hmac: &[u8],
        expected_period_ms: Option<i64>,
    ) -> bool {
        self.plan.hmac().as_bytes() == expected_hmac
            && self.plan.expiry_period_ms() == expected_period_ms
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.plan);
    }
}
