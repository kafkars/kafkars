//! Neutral terminal facts for one tracked delegation-token query.

use kafka_client_core::{DeliveryStatus, DescribeDelegationTokensPlan};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeDelegationTokenResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeDelegationTokensDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or failure for the deterministic host interpreter.
pub(crate) enum DescribeDelegationTokensTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DescribeDelegationTokenResponse,
    },
    Failed {
        kind: DescribeDelegationTokensDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response and route evidence retained through deterministic settlement.
#[must_use = "a raw DescribeDelegationTokens terminal must be deterministically settled"]
pub(crate) struct DescribeDelegationTokensRawTerminal {
    selected_version: Option<i16>,
    result: Result<DescribeDelegationTokenResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl DescribeDelegationTokensRawTerminal {
    pub(crate) fn fact(&self) -> DescribeDelegationTokensTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeDelegationTokensTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeDelegationTokensTerminalFact::Failed {
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
        } = self;
        drop(result);
        drop(route_token);
    }
}

pub(super) fn retain_describe_delegation_tokens_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeDelegationTokenResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> DescribeDelegationTokensRawTerminal {
    DescribeDelegationTokensRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> DescribeDelegationTokensDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeDelegationTokensDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeDelegationTokensDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeDelegationTokensDriverFailureKind::Compatibility
        }
        _ => DescribeDelegationTokensDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeDelegationTokens ownership still requires core settlement"]
pub(crate) struct RecoveredDescribeDelegationTokensCall {
    plan: DescribeDelegationTokensPlan,
}

impl RecoveredDescribeDelegationTokensCall {
    pub(super) const fn new(plan: DescribeDelegationTokensPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(plan: DescribeDelegationTokensPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) const fn plan(&self) -> &DescribeDelegationTokensPlan {
        &self.plan
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.plan);
    }
}
