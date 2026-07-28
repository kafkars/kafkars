//! Neutral terminal facts for one tracked SCRAM credential-description call.

use kafka_client_core::{DeliveryStatus, DescribeUserScramCredentialsPlan};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::DescribeUserScramCredentialsResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeUserScramCredentialsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum DescribeUserScramCredentialsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a DescribeUserScramCredentialsResponse,
    },
    Failed {
        kind: DescribeUserScramCredentialsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw terminal retained through protocol correlation and core settlement.
#[must_use = "a raw DescribeUserScramCredentials terminal must be deterministically settled"]
pub(crate) struct DescribeUserScramCredentialsRawTerminal {
    selected_version: Option<i16>,
    result: Result<DescribeUserScramCredentialsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: DescribeUserScramCredentialsPlan,
}

impl DescribeUserScramCredentialsRawTerminal {
    pub(crate) fn fact(&self) -> DescribeUserScramCredentialsTerminalFact<'_> {
        match &self.result {
            Ok(response) => DescribeUserScramCredentialsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => DescribeUserScramCredentialsTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    pub(crate) const fn plan(&self) -> &DescribeUserScramCredentialsPlan {
        &self.plan
    }

    /// Consumes terminal and correlation ownership after deterministic settlement.
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

pub(super) fn retain_describe_user_scram_credentials_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<DescribeUserScramCredentialsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: DescribeUserScramCredentialsPlan,
) -> DescribeUserScramCredentialsRawTerminal {
    DescribeUserScramCredentialsRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
        plan,
    }
}

fn failure_kind(error: &RequestError) -> DescribeUserScramCredentialsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => DescribeUserScramCredentialsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => DescribeUserScramCredentialsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            DescribeUserScramCredentialsDriverFailureKind::Compatibility
        }
        _ => DescribeUserScramCredentialsDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered DescribeUserScramCredentials ownership still requires core settlement"]
pub(crate) struct RecoveredDescribeUserScramCredentialsCall {
    plan: Option<DescribeUserScramCredentialsPlan>,
}

impl RecoveredDescribeUserScramCredentialsCall {
    pub(super) const fn new(plan: Option<DescribeUserScramCredentialsPlan>) -> Self {
        Self { plan }
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.plan);
    }
}
