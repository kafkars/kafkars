//! Neutral terminal facts for one tracked SCRAM credential-alteration call.

use kafka_client_core::{AlterUserScramCredentialsPlan, DeliveryStatus};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::AlterUserScramCredentialsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterUserScramCredentialsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum AlterUserScramCredentialsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a AlterUserScramCredentialsResponse,
    },
    Failed {
        kind: AlterUserScramCredentialsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained through protocol correlation and core settlement.
#[must_use = "a raw AlterUserScramCredentials terminal must be deterministically settled"]
pub(crate) struct AlterUserScramCredentialsRawTerminal {
    selected_version: Option<i16>,
    result: Result<AlterUserScramCredentialsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: AlterUserScramCredentialsPlan,
}

impl AlterUserScramCredentialsRawTerminal {
    pub(crate) fn fact(&self) -> AlterUserScramCredentialsTerminalFact<'_> {
        match &self.result {
            Ok(response) => AlterUserScramCredentialsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => AlterUserScramCredentialsTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Returns the non-secret plan used to correlate the broker response.
    pub(crate) const fn plan(&self) -> &AlterUserScramCredentialsPlan {
        &self.plan
    }

    /// Releases terminal and route ownership only after core accepts settlement.
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

pub(super) fn retain_alter_user_scram_credentials_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<AlterUserScramCredentialsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: AlterUserScramCredentialsPlan,
) -> AlterUserScramCredentialsRawTerminal {
    AlterUserScramCredentialsRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        plan,
    }
}

fn failure_kind(error: &RequestError) -> AlterUserScramCredentialsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => AlterUserScramCredentialsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => AlterUserScramCredentialsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            AlterUserScramCredentialsDriverFailureKind::Compatibility
        }
        _ => AlterUserScramCredentialsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered AlterUserScramCredentials ownership still requires core settlement"]
pub(crate) struct RecoveredAlterUserScramCredentialsCall {
    plan: Option<AlterUserScramCredentialsPlan>,
}

impl RecoveredAlterUserScramCredentialsCall {
    pub(super) const fn new(plan: Option<AlterUserScramCredentialsPlan>) -> Self {
        Self { plan }
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.plan);
    }
}
