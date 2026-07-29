//! Linear ownership of one accepted tracked controller-routed feature mutation.

mod evidence;

use std::{error::Error, fmt};

use kafka_client_core::{Moment, UpdateFeatureIntent, UpdateFeaturesPlan};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::UpdateFeaturesResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::update_features::{
        UpdateFeatureMode, UpdateFeatureRef, UpdateFeaturesRequestFailure,
        UpdateFeaturesRequestPlan, update_features_request,
    },
};

use super::{
    super::DriverOwner,
    update_features_submission::UpdateFeaturesSubmitError,
    update_features_terminal::{
        RecoveredUpdateFeaturesCall, UpdateFeaturesRawTerminal, retain_update_features_terminal,
    },
};

pub(super) use evidence::UpdateFeaturesEvidence;

/// One accepted tracked driver call retained beside its deterministic owner.
#[must_use = "an accepted UpdateFeatures call must be terminally settled"]
pub(crate) struct UpdateFeaturesCall {
    call: Option<RoutedCall<UpdateFeaturesResponse>>,
    evidence: Option<UpdateFeaturesEvidence>,
}

impl UpdateFeaturesCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: UpdateFeaturesPlan,
        result_limit: usize,
        deadline: OperationDeadline,
        now: Moment,
    ) -> Result<Self, UpdateFeaturesCallAdmissionFailure> {
        let evidence = UpdateFeaturesEvidence::new(plan, result_limit);
        let timeout_ms = match remaining_timeout_ms(now, deadline) {
            Some(timeout_ms) => timeout_ms,
            None => {
                return Err(UpdateFeaturesCallAdmissionFailure::new(
                    UpdateFeaturesAdmissionSource::Deadline,
                    evidence,
                ));
            }
        };
        let refs = match request_refs(evidence.plan()) {
            Ok(refs) => refs,
            Err(()) => {
                return Err(UpdateFeaturesCallAdmissionFailure::new(
                    UpdateFeaturesAdmissionSource::Request,
                    evidence,
                ));
            }
        };
        let request = update_features_request(
            UpdateFeaturesRequestPlan::new(&refs, evidence.plan().validate_only()),
            timeout_ms,
            evidence.result_limit(),
        );
        drop(refs);
        let (request, minimum_version) = match request {
            Ok(request) => request,
            Err(source) => {
                return Err(UpdateFeaturesCallAdmissionFailure::new(
                    UpdateFeaturesAdmissionSource::Protocol(source),
                    evidence,
                ));
            }
        };
        let call = match driver.submit_tracked_update_features(
            request,
            minimum_version,
            deadline.transport(),
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(UpdateFeaturesCallAdmissionFailure::new(
                    UpdateFeaturesAdmissionSource::Driver(source),
                    evidence,
                ));
            }
        };
        Ok(Self {
            call: Some(call),
            evidence: Some(evidence),
        })
    }

    /// Extracts one ready raw terminal without releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<UpdateFeaturesRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_update_features_terminal(
                    selected_version,
                    result,
                    route_token,
                    evidence,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn matches_evidence(&self, plan: &UpdateFeaturesPlan, result_limit: usize) -> bool {
        self.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.matches(plan, result_limit))
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredUpdateFeaturesCall> {
        let Self { call, evidence } = self;
        call.zip(evidence).map(|(call, evidence)| {
            drop(call);
            RecoveredUpdateFeaturesCall::new(evidence)
        })
    }
}

fn remaining_timeout_ms(now: Moment, deadline: OperationDeadline) -> Option<i32> {
    let remaining = deadline
        .core()
        .tick()
        .checked_sub(now.tick())
        .filter(|remaining| *remaining > 0)?;
    let milliseconds = remaining.saturating_add(999_999) / 1_000_000;
    Some(i32::try_from(milliseconds).unwrap_or(i32::MAX))
}

fn request_refs(plan: &UpdateFeaturesPlan) -> Result<Vec<UpdateFeatureRef<'_>>, ()> {
    let mut refs = Vec::new();
    refs.try_reserve_exact(plan.updates().len())
        .map_err(|_| ())?;
    refs.extend(plan.updates().iter().map(|update| {
        UpdateFeatureRef::new(
            update.feature(),
            update.max_version_level(),
            match update.intent() {
                UpdateFeatureIntent::Upgrade => UpdateFeatureMode::Upgrade,
                UpdateFeatureIntent::SafeDowngrade => UpdateFeatureMode::SafeDowngrade,
                UpdateFeatureIntent::UnsafeDowngrade => UpdateFeatureMode::UnsafeDowngrade,
            },
        )
    }));
    Ok(refs)
}

#[derive(Debug)]
enum UpdateFeaturesAdmissionSource {
    Deadline,
    Request,
    Protocol(UpdateFeaturesRequestFailure),
    Driver(UpdateFeaturesSubmitError),
}

/// Definitely-unsent rejection retaining the exact attempted feature mutation.
#[derive(Debug)]
#[must_use = "a rejected UpdateFeatures call must become deterministic input"]
pub(crate) struct UpdateFeaturesCallAdmissionFailure {
    source: UpdateFeaturesAdmissionSource,
    evidence: UpdateFeaturesEvidence,
}

impl UpdateFeaturesCallAdmissionFailure {
    const fn new(source: UpdateFeaturesAdmissionSource, evidence: UpdateFeaturesEvidence) -> Self {
        Self { source, evidence }
    }

    pub(crate) fn into_submission_evidence(self) -> (UpdateFeaturesPlan, usize) {
        let Self { source, evidence } = self;
        drop(source);
        evidence.into_parts()
    }
}

impl fmt::Display for UpdateFeaturesCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            UpdateFeaturesAdmissionSource::Deadline => {
                formatter.write_str("UpdateFeatures deadline elapsed before driver submission")
            }
            UpdateFeaturesAdmissionSource::Request => {
                formatter.write_str("UpdateFeatures request reference allocation failed")
            }
            UpdateFeaturesAdmissionSource::Protocol(source) => {
                write!(formatter, "UpdateFeatures request rejected: {source:?}")
            }
            UpdateFeaturesAdmissionSource::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for UpdateFeaturesCallAdmissionFailure {}
