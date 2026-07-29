//! Neutral terminal facts and causal route refresh for one feature mutation.

mod refresh;

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::UpdateFeaturesResponse;

use super::{
    super::{DriverOwner, request_failure_delivery},
    update_features_call::UpdateFeaturesEvidence,
};
use refresh::UpdateFeaturesControllerRefresh;

pub(crate) use refresh::UpdateFeaturesControllerRefreshPoll;

/// Stable engine-local classification without exposing driver variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpdateFeaturesDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed response or driver failure for the deterministic host interpreter.
pub(crate) enum UpdateFeaturesTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a UpdateFeaturesResponse,
    },
    Failed {
        kind: UpdateFeaturesDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Raw response ownership retained through normalization and core settlement.
#[must_use = "a raw UpdateFeatures terminal must be deterministically settled"]
pub(crate) struct UpdateFeaturesRawTerminal {
    selected_version: Option<i16>,
    result: Result<UpdateFeaturesResponse, RequestError>,
    controller_refresh: UpdateFeaturesControllerRefresh,
    evidence: UpdateFeaturesEvidence,
}

impl UpdateFeaturesRawTerminal {
    pub(crate) fn matches_evidence(
        &self,
        plan: &kafka_client_core::UpdateFeaturesPlan,
        result_limit: usize,
    ) -> bool {
        self.evidence.matches(plan, result_limit)
    }

    pub(crate) const fn response_plan(&self) -> &kafka_client_core::UpdateFeaturesPlan {
        self.evidence.plan()
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.evidence.result_limit()
    }

    pub(crate) fn fact(&self) -> UpdateFeaturesTerminalFact<'_> {
        match &self.result {
            Ok(response) => UpdateFeaturesTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => UpdateFeaturesTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    /// Advances at most one controller-invalidation transition without replaying the mutation.
    pub(crate) fn poll_controller_refresh(
        &mut self,
        driver: Option<&DriverOwner>,
    ) -> UpdateFeaturesControllerRefreshPoll {
        self.controller_refresh.poll(driver)
    }

    #[cfg(test)]
    pub(super) fn arm_controller_refresh_for_test(&mut self) {
        self.controller_refresh.arm_for_test();
    }

    /// Releases response and route evidence only after deterministic settlement.
    pub(crate) fn discard(self) {
        let Self {
            selected_version: _,
            result,
            controller_refresh,
            evidence,
        } = self;
        drop((result, controller_refresh, evidence));
    }
}

pub(super) fn retain_update_features_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<UpdateFeaturesResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: UpdateFeaturesEvidence,
) -> UpdateFeaturesRawTerminal {
    let selected_version = selected_version.map(ApiVersion::value);
    let controller_refresh =
        UpdateFeaturesControllerRefresh::from_terminal(selected_version, &result, route_token);
    UpdateFeaturesRawTerminal {
        selected_version,
        result,
        controller_refresh,
        evidence,
    }
}

pub(super) fn response_requires_controller_refresh(
    selected_version: Option<i16>,
    result: &Result<UpdateFeaturesResponse, RequestError>,
) -> bool {
    matches!(
        (selected_version, result),
        (Some(0..=2), Ok(response)) if response.error_code == 41
    )
}

fn failure_kind(error: &RequestError) -> UpdateFeaturesDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => UpdateFeaturesDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => UpdateFeaturesDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            UpdateFeaturesDriverFailureKind::Compatibility
        }
        _ => UpdateFeaturesDriverFailureKind::Transport,
    }
}

/// Accepted ownership recovered only after the unique driver is destroyed.
#[must_use = "recovered UpdateFeatures ownership still requires core settlement"]
pub(crate) struct RecoveredUpdateFeaturesCall {
    evidence: UpdateFeaturesEvidence,
}

impl RecoveredUpdateFeaturesCall {
    pub(super) const fn new(evidence: UpdateFeaturesEvidence) -> Self {
        Self { evidence }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        plan: kafka_client_core::UpdateFeaturesPlan,
        result_limit: usize,
    ) -> Self {
        Self {
            evidence: UpdateFeaturesEvidence::new(plan, result_limit),
        }
    }

    pub(crate) fn matches_evidence(
        &self,
        plan: &kafka_client_core::UpdateFeaturesPlan,
        result_limit: usize,
    ) -> bool {
        self.evidence.matches(plan, result_limit)
    }

    /// Consumes recovered ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        drop(self.evidence);
    }
}
