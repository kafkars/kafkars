//! Neutral terminal facts and causal controller refresh for one voter removal.

use std::mem;

use kafka_client_core::{DeliveryStatus, RemoveRaftVoterPlan};
use kafka_driver::{
    ApiVersion, Call, CallFailure, InvalidationDisposition, RequestError, RouteFailureToken,
    RouteKind,
};
use kafka_wire::RemoveRaftVoterResponse;

use super::super::{DriverOwner, request_failure_delivery};

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemoveRaftVoterDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the deterministic host interpreter.
pub(crate) enum RemoveRaftVoterTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a RemoveRaftVoterResponse,
    },
    Failed {
        kind: RemoveRaftVoterDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw RemoveRaftVoter terminal must be deterministically settled"]
pub(crate) struct RemoveRaftVoterRawTerminal {
    selected_version: Option<i16>,
    result: Result<RemoveRaftVoterResponse, RequestError>,
    controller_refresh: RemoveRaftVoterControllerRefresh,
    plan: RemoveRaftVoterPlan,
}

enum RemoveRaftVoterControllerRefresh {
    None,
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
    #[cfg(test)]
    QueuedForTest,
    #[cfg(test)]
    ActiveForTest {
        completion_ready: bool,
    },
}

impl RemoveRaftVoterRawTerminal {
    #[cfg(test)]
    pub(crate) fn not_controller_for_test(plan: RemoveRaftVoterPlan) -> Self {
        let mut response = RemoveRaftVoterResponse::default();
        response.error_code = 41;
        Self {
            selected_version: Some(0),
            result: Ok(response),
            controller_refresh: RemoveRaftVoterControllerRefresh::QueuedForTest,
            plan,
        }
    }

    pub(crate) fn fact(&self) -> RemoveRaftVoterTerminalFact<'_> {
        match &self.result {
            Ok(response) => RemoveRaftVoterTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => RemoveRaftVoterTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Advances at most one causal invalidation transition without replaying API 81.
    ///
    /// `Some(true)` means the barrier is clear, `Some(false)` means it retained
    /// pending work, and `None` means a queued refresh has no live driver owner.
    pub(crate) fn poll_controller_refresh(&mut self, driver: Option<&DriverOwner>) -> Option<bool> {
        self.controller_refresh.poll(driver)
    }

    #[cfg(test)]
    pub(super) fn arm_controller_refresh_for_test(&mut self) {
        self.controller_refresh = RemoveRaftVoterControllerRefresh::QueuedForTest;
    }

    #[cfg(test)]
    pub(crate) fn matches_plan_for_test(&self, expected: &RemoveRaftVoterPlan) -> bool {
        &self.plan == expected
    }

    /// Releases response and route evidence only after deterministic settlement.
    pub(crate) fn discard(self) {
        let Self {
            selected_version: _,
            result,
            controller_refresh,
            plan,
        } = self;
        drop(result);
        drop(controller_refresh);
        drop(plan);
    }
}

impl RemoveRaftVoterControllerRefresh {
    fn poll(&mut self, driver: Option<&DriverOwner>) -> Option<bool> {
        match mem::replace(self, Self::None) {
            Self::None => Some(true),
            Self::Queued(route_token) => {
                let Some(driver) = driver else {
                    *self = Self::Queued(route_token);
                    return None;
                };
                match driver.driver.invalidate(route_token) {
                    Ok(call) => *self = Self::Active(call),
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        *self = Self::Queued(route_token);
                    }
                }
                Some(false)
            }
            Self::Active(call) => {
                if call.try_result().is_none() {
                    *self = Self::Active(call);
                    Some(false)
                } else {
                    Some(true)
                }
            }
            #[cfg(test)]
            Self::QueuedForTest => {
                if driver.is_none() {
                    *self = Self::QueuedForTest;
                    None
                } else {
                    *self = Self::ActiveForTest {
                        completion_ready: false,
                    };
                    Some(false)
                }
            }
            #[cfg(test)]
            Self::ActiveForTest { completion_ready } => {
                if completion_ready {
                    Some(true)
                } else {
                    *self = Self::ActiveForTest {
                        completion_ready: true,
                    };
                    Some(false)
                }
            }
        }
    }
}

pub(super) fn retain_remove_raft_voter_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<RemoveRaftVoterResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: RemoveRaftVoterPlan,
) -> RemoveRaftVoterRawTerminal {
    let selected_version = selected_version.map(ApiVersion::value);
    let controller_refresh = if response_requires_controller_refresh(selected_version, &result) {
        match route_token {
            Some(route_token) if route_token.kind() == RouteKind::Controller => {
                RemoveRaftVoterControllerRefresh::Queued(route_token)
            }
            route_token => {
                drop(route_token);
                RemoveRaftVoterControllerRefresh::None
            }
        }
    } else {
        drop(route_token);
        RemoveRaftVoterControllerRefresh::None
    };
    RemoveRaftVoterRawTerminal {
        selected_version,
        result,
        controller_refresh,
        plan,
    }
}

pub(super) fn response_requires_controller_refresh(
    selected_version: Option<i16>,
    result: &Result<RemoveRaftVoterResponse, RequestError>,
) -> bool {
    matches!(
        (selected_version, result),
        (Some(0), Ok(response)) if response.error_code == 41
    )
}

fn failure_kind(error: &RequestError) -> RemoveRaftVoterDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => RemoveRaftVoterDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => RemoveRaftVoterDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            RemoveRaftVoterDriverFailureKind::Compatibility
        }
        _ => RemoveRaftVoterDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered RemoveRaftVoter ownership still requires core settlement"]
pub(crate) struct RecoveredRemoveRaftVoterCall {
    plan: RemoveRaftVoterPlan,
}

impl RecoveredRemoveRaftVoterCall {
    pub(super) const fn new(plan: RemoveRaftVoterPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(plan: RemoveRaftVoterPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) fn matches_plan_for_test(&self, expected: &RemoveRaftVoterPlan) -> bool {
        &self.plan == expected
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.plan);
    }
}
