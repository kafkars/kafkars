//! Neutral terminal facts for one tracked delegation-token creation.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::CreateDelegationTokenResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateDelegationTokenDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the deterministic host interpreter.
pub(crate) enum CreateDelegationTokenTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a CreateDelegationTokenResponse,
    },
    Failed {
        kind: CreateDelegationTokenDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw CreateDelegationToken terminal must be deterministically settled"]
pub(crate) struct CreateDelegationTokenRawTerminal {
    selected_version: Option<i16>,
    result: Result<CreateDelegationTokenResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl CreateDelegationTokenRawTerminal {
    pub(crate) fn fact(&self) -> CreateDelegationTokenTerminalFact<'_> {
        match &self.result {
            Ok(response) => CreateDelegationTokenTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => CreateDelegationTokenTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Releases response and route evidence only after deterministic settlement.
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

pub(super) fn retain_create_delegation_token_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<CreateDelegationTokenResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> CreateDelegationTokenRawTerminal {
    CreateDelegationTokenRawTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> CreateDelegationTokenDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => CreateDelegationTokenDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => CreateDelegationTokenDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            CreateDelegationTokenDriverFailureKind::Compatibility
        }
        _ => CreateDelegationTokenDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered CreateDelegationToken ownership still requires core settlement"]
pub(crate) struct RecoveredCreateDelegationTokenCall {
    _private: (),
}

impl RecoveredCreateDelegationTokenCall {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    #[cfg(test)]
    pub(crate) const fn for_test() -> Self {
        Self { _private: () }
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) const fn seal(self) {
        let Self { _private: () } = self;
    }
}
