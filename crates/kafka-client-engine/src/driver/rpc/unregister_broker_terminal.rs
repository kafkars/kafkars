//! Neutral terminal facts and causal controller refresh for one broker unregistration.

mod refresh;

use kafka_client_core::{DeliveryStatus, UnregisterBrokerPlan};
use kafka_driver::{ApiVersion, CallFailure, Delivery, RequestError, RouteFailureToken};
use kafka_wire::UnregisterBrokerResponse;

use super::super::{DriverOwner, request_failure_delivery};
use refresh::UnregisterBrokerControllerRefresh;

pub(crate) use refresh::UnregisterBrokerControllerRefreshPoll;

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

impl UnregisterBrokerRawTerminal {
    #[cfg(test)]
    pub(crate) fn controller_route_unavailable_for_test(plan: UnregisterBrokerPlan) -> Self {
        Self {
            selected_version: None,
            result: Err(RequestError::Rejected {
                failure: CallFailure::NotReady,
                delivery: Delivery::NotSent,
            }),
            controller_refresh: UnregisterBrokerControllerRefresh::for_test(true),
            plan,
        }
    }

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
    pub(crate) fn poll_controller_refresh(
        &mut self,
        driver: Option<&DriverOwner>,
    ) -> UnregisterBrokerControllerRefreshPoll {
        self.controller_refresh.poll(driver)
    }

    #[cfg(test)]
    pub(super) fn arm_controller_refresh_for_test(&mut self, retry: bool) {
        self.controller_refresh.arm_for_test(retry);
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

pub(super) fn retain_unregister_broker_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<UnregisterBrokerResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: UnregisterBrokerPlan,
) -> UnregisterBrokerRawTerminal {
    let selected_version = selected_version.map(ApiVersion::value);
    let controller_refresh =
        UnregisterBrokerControllerRefresh::from_terminal(selected_version, &result, route_token);
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

pub(super) fn request_requires_controller_retry(
    result: &Result<UnregisterBrokerResponse, RequestError>,
) -> bool {
    matches!(
        result,
        Err(RequestError::Rejected {
            failure: CallFailure::NotReady,
            delivery: Delivery::NotSent,
        })
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
