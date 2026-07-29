//! Neutral terminal facts retaining route evidence for legacy resource configuration replacement.

use kafka_client_core::{DeliveryStatus, LegacyAlterConfigsPlan, LegacyAlterConfigsRoute};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::AlterConfigsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification independent of driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LegacyAlterConfigsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the concrete host interpreter.
pub(crate) enum LegacyAlterConfigsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a AlterConfigsResponse,
    },
    Failed {
        kind: LegacyAlterConfigsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained through normalization and core settlement.
#[must_use = "a raw legacy AlterConfigs terminal owns unsettled route evidence"]
pub(crate) struct LegacyAlterConfigsTerminal {
    selected_version: Option<i16>,
    result: Result<AlterConfigsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    route: LegacyAlterConfigsRoute,
    plan: LegacyAlterConfigsPlan,
}

impl LegacyAlterConfigsTerminal {
    pub(crate) fn fact(&self) -> LegacyAlterConfigsTerminalFact<'_> {
        match &self.result {
            Ok(response) => LegacyAlterConfigsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => LegacyAlterConfigsTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Returns the exact destination retained for deterministic correlation.
    pub(crate) const fn route(&self) -> LegacyAlterConfigsRoute {
        self.route
    }

    /// Returns the exact caller-ordered configuration plan sent on this route.
    pub(crate) const fn plan(&self) -> &LegacyAlterConfigsPlan {
        &self.plan
    }

    /// Releases response and route evidence only after deterministic settlement.
    pub(crate) fn discard(self) {
        let Self {
            selected_version: _,
            result,
            route_token,
            route,
            plan,
        } = self;
        drop(result);
        drop(route_token);
        let _ = route;
        drop(plan);
    }
}

pub(super) fn retain_legacy_alter_configs_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<AlterConfigsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    route: LegacyAlterConfigsRoute,
    plan: LegacyAlterConfigsPlan,
) -> LegacyAlterConfigsTerminal {
    LegacyAlterConfigsTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        route,
        plan,
    }
}

fn failure_kind(error: &RequestError) -> LegacyAlterConfigsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => LegacyAlterConfigsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => LegacyAlterConfigsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            LegacyAlterConfigsDriverFailureKind::Compatibility
        }
        _ => LegacyAlterConfigsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after unique driver destruction.
#[must_use = "recovered legacy AlterConfigs ownership still requires deterministic settlement"]
pub(crate) struct RecoveredLegacyAlterConfigsCall {
    route: LegacyAlterConfigsRoute,
    plan: LegacyAlterConfigsPlan,
}

impl RecoveredLegacyAlterConfigsCall {
    pub(super) const fn new(route: LegacyAlterConfigsRoute, plan: LegacyAlterConfigsPlan) -> Self {
        Self { route, plan }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        route: LegacyAlterConfigsRoute,
        plan: LegacyAlterConfigsPlan,
    ) -> Self {
        Self { route, plan }
    }

    pub(crate) fn matches_correlation(
        &self,
        expected_route: LegacyAlterConfigsRoute,
        expected_plan: &LegacyAlterConfigsPlan,
    ) -> bool {
        self.route == expected_route && &self.plan == expected_plan
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        let Self { route, plan } = self;
        let _ = route;
        drop(plan);
    }
}
