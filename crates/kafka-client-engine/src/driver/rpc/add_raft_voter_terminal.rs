//! Neutral terminal facts and causal controller refresh for one voter addition.

mod refresh;

use kafka_client_core::{AddRaftVoterPlan, DeliveryStatus};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::AddRaftVoterResponse;

use super::super::{DriverOwner, request_failure_delivery};
use refresh::AddRaftVoterControllerRefresh;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddRaftVoterDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the deterministic host interpreter.
pub(crate) enum AddRaftVoterTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a AddRaftVoterResponse,
    },
    Failed {
        kind: AddRaftVoterDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw AddRaftVoter terminal must be deterministically settled"]
pub(crate) struct AddRaftVoterRawTerminal {
    selected_version: Option<i16>,
    result: Result<AddRaftVoterResponse, RequestError>,
    controller_refresh: AddRaftVoterControllerRefresh,
    plan: AddRaftVoterPlan,
}

impl AddRaftVoterRawTerminal {
    #[cfg(test)]
    pub(crate) fn not_controller_for_test(plan: AddRaftVoterPlan) -> Self {
        let mut response = AddRaftVoterResponse::default();
        response.error_code = 41;
        Self {
            selected_version: Some(1),
            result: Ok(response),
            controller_refresh: AddRaftVoterControllerRefresh::queued_for_test(),
            plan,
        }
    }

    pub(crate) fn fact(&self) -> AddRaftVoterTerminalFact<'_> {
        match &self.result {
            Ok(response) => AddRaftVoterTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => AddRaftVoterTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Advances at most one causal invalidation transition without replaying API 80.
    ///
    /// `Some(true)` means the barrier is clear, `Some(false)` means it retained
    /// pending work, and `None` means a queued refresh has no live driver owner.
    pub(crate) fn poll_controller_refresh(&mut self, driver: Option<&DriverOwner>) -> Option<bool> {
        self.controller_refresh.poll(driver)
    }

    #[cfg(test)]
    pub(super) fn arm_controller_refresh_for_test(&mut self) {
        self.controller_refresh.arm_for_test();
    }

    #[cfg(test)]
    pub(crate) const fn plan(&self) -> &AddRaftVoterPlan {
        &self.plan
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

pub(super) fn retain_add_raft_voter_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<AddRaftVoterResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    plan: AddRaftVoterPlan,
) -> AddRaftVoterRawTerminal {
    let selected_version = selected_version.map(ApiVersion::value);
    let controller_refresh =
        AddRaftVoterControllerRefresh::from_terminal(selected_version, &result, route_token);
    AddRaftVoterRawTerminal {
        selected_version,
        result,
        controller_refresh,
        plan,
    }
}

pub(super) fn response_requires_controller_refresh(
    selected_version: Option<i16>,
    result: &Result<AddRaftVoterResponse, RequestError>,
) -> bool {
    matches!(
        (selected_version, result),
        (Some(0..=1), Ok(response)) if response.error_code == 41
    )
}

fn failure_kind(error: &RequestError) -> AddRaftVoterDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => AddRaftVoterDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => AddRaftVoterDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => AddRaftVoterDriverFailureKind::Compatibility,
        _ => AddRaftVoterDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered AddRaftVoter ownership still requires core settlement"]
pub(crate) struct RecoveredAddRaftVoterCall {
    plan: AddRaftVoterPlan,
}

impl RecoveredAddRaftVoterCall {
    pub(super) const fn new(plan: AddRaftVoterPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(plan: AddRaftVoterPlan) -> Self {
        Self { plan }
    }

    #[cfg(test)]
    pub(crate) const fn plan(&self) -> &AddRaftVoterPlan {
        &self.plan
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.plan);
    }
}
