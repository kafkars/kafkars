//! Neutral terminal facts and causal controller refresh for one broker unregistration.

use std::mem;

use kafka_client_core::{DeliveryStatus, UnregisterBrokerPlan};
use kafka_driver::{
    ApiVersion, Call, CallFailure, InvalidationDisposition, RequestError, RouteFailureToken,
    RouteKind,
};
use kafka_wire::UnregisterBrokerResponse;

use super::super::{DriverOwner, request_failure_delivery};

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnregisterBrokerDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the deterministic host interpreter.
pub(crate) enum UnregisterBrokerTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a UnregisterBrokerResponse,
    },
    Failed {
        kind: UnregisterBrokerDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw UnregisterBroker terminal must be deterministically settled"]
pub(crate) struct UnregisterBrokerRawTerminal {
    selected_version: Option<i16>,
    result: Result<UnregisterBrokerResponse, RequestError>,
    controller_refresh: UnregisterBrokerControllerRefresh,
    plan: UnregisterBrokerPlan,
}

enum UnregisterBrokerControllerRefresh {
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

impl UnregisterBrokerRawTerminal {
    pub(crate) fn fact(&self) -> UnregisterBrokerTerminalFact<'_> {
        match &self.result {
            Ok(response) => UnregisterBrokerTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => UnregisterBrokerTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Advances at most one causal invalidation transition without replaying the mutation.
    ///
    /// `Some(true)` means the barrier is clear, `Some(false)` means it retained
    /// pending work, and `None` means a queued refresh has no live driver owner.
    pub(crate) fn poll_controller_refresh(&mut self, driver: Option<&DriverOwner>) -> Option<bool> {
        self.controller_refresh.poll(driver)
    }

    #[cfg(test)]
    pub(super) fn arm_controller_refresh_for_test(&mut self) {
        self.controller_refresh = UnregisterBrokerControllerRefresh::QueuedForTest;
    }

    #[cfg(test)]
    pub(crate) const fn broker_id_for_test(&self) -> i32 {
        self.plan.broker_id()
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
        let _ = plan;
    }
}

impl UnregisterBrokerControllerRefresh {
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

pub(super) fn retain_unregister_broker_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<UnregisterBrokerResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: UnregisterBrokerPlan,
) -> UnregisterBrokerRawTerminal {
    let selected_version = selected_version.map(ApiVersion::value);
    let controller_refresh = if response_requires_controller_refresh(selected_version, &result) {
        match route_token {
            Some(route_token) if route_token.kind() == RouteKind::Controller => {
                UnregisterBrokerControllerRefresh::Queued(route_token)
            }
            route_token => {
                drop(route_token);
                UnregisterBrokerControllerRefresh::None
            }
        }
    } else {
        drop(route_token);
        UnregisterBrokerControllerRefresh::None
    };
    UnregisterBrokerRawTerminal {
        selected_version,
        result,
        controller_refresh,
        plan,
    }
}

pub(super) fn response_requires_controller_refresh(
    selected_version: Option<i16>,
    result: &Result<UnregisterBrokerResponse, RequestError>,
) -> bool {
    matches!(
        (selected_version, result),
        (Some(0), Ok(response)) if response.error_code == 41
    )
}

fn failure_kind(error: &RequestError) -> UnregisterBrokerDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => UnregisterBrokerDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => UnregisterBrokerDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            UnregisterBrokerDriverFailureKind::Compatibility
        }
        _ => UnregisterBrokerDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered UnregisterBroker ownership still requires core settlement"]
pub(crate) struct RecoveredUnregisterBrokerCall {
    plan: UnregisterBrokerPlan,
}

impl RecoveredUnregisterBrokerCall {
    pub(super) const fn new(plan: UnregisterBrokerPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(plan: UnregisterBrokerPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) const fn broker_id_for_test(&self) -> i32 {
        self.plan.broker_id()
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        let _ = self.plan;
    }
}
