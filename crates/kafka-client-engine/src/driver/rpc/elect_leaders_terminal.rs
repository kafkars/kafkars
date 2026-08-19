//! Raw election terminal facts with linear route-receipt ownership.

use std::mem;

use kafka_client_core::DeliveryStatus;
use kafka_driver::{
    ApiVersion, Call, CallFailure, Driver, InvalidationDisposition, RequestError, RouteFailureToken,
};
use kafka_wire::ElectLeadersResponse;

use super::{super::request_failure_delivery, elect_leaders_call::ElectLeadersCorrelation};

/// Stable engine-local failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ElectLeadersDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure safe for the concrete host interpreter.
pub(crate) enum ElectLeadersTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a ElectLeadersResponse,
    },
    Failed {
        kind: ElectLeadersDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained until deterministic settlement.
#[must_use = "a raw election terminal owns unsettled route evidence"]
pub(crate) struct ElectLeadersTerminal {
    selected_version: Option<i16>,
    result: Result<ElectLeadersResponse, RequestError>,
    controller_refresh: ElectLeadersControllerRefresh,
    correlation: ElectLeadersCorrelation,
}

enum ElectLeadersControllerRefresh {
    None,
    Queued(Driver, RouteFailureToken),
    Active(Call<InvalidationDisposition>),
}

/// One bounded observation of causal controller-route refresh progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ElectLeadersControllerRefreshPoll {
    Ready,
    Pending,
    Progress,
}

impl ElectLeadersTerminal {
    pub(crate) fn fact(&self) -> ElectLeadersTerminalFact<'_> {
        match &self.result {
            Ok(response) => ElectLeadersTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => ElectLeadersTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    pub(crate) const fn plan(&self) -> &kafka_client_core::ElectLeadersPlan {
        self.correlation.plan()
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.correlation.result_limit()
    }

    pub(crate) fn matches_correlation(
        &self,
        plan: &kafka_client_core::ElectLeadersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.correlation
            .matches(plan, request_scratch_limit, result_limit)
    }

    pub(crate) fn poll_controller_refresh(&mut self) -> ElectLeadersControllerRefreshPoll {
        match mem::replace(
            &mut self.controller_refresh,
            ElectLeadersControllerRefresh::None,
        ) {
            ElectLeadersControllerRefresh::None => ElectLeadersControllerRefreshPoll::Ready,
            ElectLeadersControllerRefresh::Queued(driver, route_token) => {
                match driver.invalidate(route_token) {
                    Ok(call) => {
                        self.controller_refresh = ElectLeadersControllerRefresh::Active(call);
                        ElectLeadersControllerRefreshPoll::Progress
                    }
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        self.controller_refresh =
                            ElectLeadersControllerRefresh::Queued(driver, route_token);
                        ElectLeadersControllerRefreshPoll::Pending
                    }
                }
            }
            ElectLeadersControllerRefresh::Active(call) => {
                if call.try_result().is_none() {
                    self.controller_refresh = ElectLeadersControllerRefresh::Active(call);
                    ElectLeadersControllerRefreshPoll::Pending
                } else {
                    ElectLeadersControllerRefreshPoll::Ready
                }
            }
        }
    }

    /// Releases route evidence only after core terminal settlement.
    pub(crate) fn discard(self) {
        let Self {
            selected_version: _,
            result,
            controller_refresh,
            correlation,
        } = self;
        drop(result);
        drop(controller_refresh);
        drop(correlation);
    }
}

pub(super) fn retain_elect_leaders_terminal(
    driver: Driver,
    selected_version: Option<ApiVersion>,
    result: Result<ElectLeadersResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    correlation: ElectLeadersCorrelation,
) -> ElectLeadersTerminal {
    let selected_version = selected_version.map(ApiVersion::value);
    let controller_refresh = if response_requires_controller_refresh(selected_version, &result) {
        if let Some(route_token) = route_token {
            ElectLeadersControllerRefresh::Queued(driver, route_token)
        } else {
            drop(driver);
            ElectLeadersControllerRefresh::None
        }
    } else {
        drop((driver, route_token));
        ElectLeadersControllerRefresh::None
    };
    ElectLeadersTerminal {
        selected_version,
        result,
        controller_refresh,
        correlation,
    }
}

pub(super) fn response_requires_controller_refresh(
    selected_version: Option<i16>,
    result: &Result<ElectLeadersResponse, RequestError>,
) -> bool {
    matches!(
        (selected_version, result),
        (Some(0..=2), Ok(response)) if response.error_code == 41
    )
}

fn failure_kind(error: &RequestError) -> ElectLeadersDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => ElectLeadersDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => ElectLeadersDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => ElectLeadersDriverFailureKind::Compatibility,
        _ => ElectLeadersDriverFailureKind::Transport,
    }
}

/// Accepted call recovered only after the unique driver is destroyed.
#[must_use = "recovered election ownership still requires core settlement"]
pub(crate) struct RecoveredElectLeadersCall {
    correlation: ElectLeadersCorrelation,
}

impl RecoveredElectLeadersCall {
    pub(super) const fn new(correlation: ElectLeadersCorrelation) -> Self {
        Self { correlation }
    }

    pub(crate) fn matches_correlation(
        &self,
        plan: &kafka_client_core::ElectLeadersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.correlation
            .matches(plan, request_scratch_limit, result_limit)
    }

    pub(crate) fn seal(self) {
        drop(self.correlation);
    }
}
