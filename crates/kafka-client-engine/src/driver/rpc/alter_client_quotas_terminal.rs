//! Neutral terminal facts for one tracked AnyBroker `AlterClientQuotas` call.

use kafka_client_core::AlterClientQuotasPlan;
use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::AlterClientQuotasResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterClientQuotasDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum AlterClientQuotasTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a AlterClientQuotasResponse,
    },
    Failed {
        kind: AlterClientQuotasDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained through protocol validation and core settlement.
#[must_use = "a raw AlterClientQuotas terminal must be deterministically settled"]
pub(crate) struct AlterClientQuotasRawTerminal {
    selected_version: Option<i16>,
    result: Result<AlterClientQuotasResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: AlterClientQuotasPlan,
}

impl AlterClientQuotasRawTerminal {
    pub(crate) fn fact(&self) -> AlterClientQuotasTerminalFact<'_> {
        match &self.result {
            Ok(response) => AlterClientQuotasTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => AlterClientQuotasTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Returns the exact validated request plan used for response correlation.
    pub(crate) const fn plan(&self) -> &AlterClientQuotasPlan {
        &self.plan
    }

    /// Consumes terminal ownership after deterministic settlement.
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

pub(super) fn retain_alter_client_quotas_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<AlterClientQuotasResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: AlterClientQuotasPlan,
) -> AlterClientQuotasRawTerminal {
    AlterClientQuotasRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        plan,
    }
}

fn failure_kind(error: &RequestError) -> AlterClientQuotasDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => AlterClientQuotasDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => AlterClientQuotasDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            AlterClientQuotasDriverFailureKind::Compatibility
        }
        _ => AlterClientQuotasDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered AlterClientQuotas ownership still requires core settlement"]
pub(crate) struct RecoveredAlterClientQuotasCall {
    _private: (),
}

impl RecoveredAlterClientQuotasCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
