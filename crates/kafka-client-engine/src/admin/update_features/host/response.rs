//! Exhaustive normalized-protocol and driver-failure translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, UpdateFeatureIntent, UpdateFeatureOutcome, UpdateFeaturesBatch,
    UpdateFeaturesBrokerError, UpdateFeaturesBrokerResponse, UpdateFeaturesInput,
    UpdateFeaturesPlan,
};

use crate::{
    driver::{
        UpdateFeaturesDriverFailureKind, UpdateFeaturesRawTerminal, UpdateFeaturesTerminalFact,
    },
    protocol::admin::update_features::{
        NormalizedUpdateFeatureResult, NormalizedUpdateFeaturesError,
        NormalizedUpdateFeaturesOutcome, UpdateFeatureMode, UpdateFeatureRef,
        UpdateFeaturesProtocolFailure, UpdateFeaturesRequestPlan,
        normalize_update_features_response,
    },
};

pub(super) fn terminal_input(raw: &UpdateFeaturesRawTerminal) -> (UpdateFeaturesInput, usize) {
    let plan = raw.response_plan();
    let retained_limit = raw.result_limit();
    match raw.fact() {
        UpdateFeaturesTerminalFact::Response {
            selected_version,
            response,
        } => {
            let refs = match request_refs(plan) {
                Ok(refs) => refs,
                Err(()) => return (UpdateFeaturesInput::ResponseTooLarge, 0),
            };
            let request_plan = UpdateFeaturesRequestPlan::new(&refs, plan.validate_only());
            match normalize_update_features_response(
                selected_version,
                response,
                request_plan,
                retained_limit,
            ) {
                Ok(normalized) => {
                    let (throttle_time_ms, outcome, retained_bytes) = normalized.into_parts();
                    (normalized_input(throttle_time_ms, outcome), retained_bytes)
                }
                Err(error) => (protocol_failure(error), 0),
            }
        }
        UpdateFeaturesTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) fn normalized_input(
    throttle_time_ms: u32,
    outcome: NormalizedUpdateFeaturesOutcome,
) -> UpdateFeaturesInput {
    match outcome {
        NormalizedUpdateFeaturesOutcome::TopLevelError(error) => match core_error(error) {
            Some(error) => UpdateFeaturesInput::BrokerRejected { error },
            None => UpdateFeaturesInput::InvalidResponse,
        },
        NormalizedUpdateFeaturesOutcome::Results(results) => {
            let mut outcomes = Vec::new();
            if outcomes.try_reserve_exact(results.len()).is_err() {
                return UpdateFeaturesInput::ResponseTooLarge;
            }
            for result in results {
                let Some(outcome) = core_outcome(result) else {
                    return UpdateFeaturesInput::InvalidResponse;
                };
                outcomes.push(outcome);
            }
            UpdateFeaturesInput::BrokerResponded {
                response: UpdateFeaturesBrokerResponse::FeatureResults(UpdateFeaturesBatch::new(
                    throttle_time_ms,
                    outcomes,
                )),
            }
        }
    }
}

fn core_outcome(result: NormalizedUpdateFeatureResult) -> Option<UpdateFeatureOutcome> {
    let (feature, error) = result.into_parts();
    match error {
        Some(error) => Some(UpdateFeatureOutcome::failed(feature, core_error(error)?)),
        None => Some(UpdateFeatureOutcome::updated(feature)),
    }
}

fn core_error(error: NormalizedUpdateFeaturesError) -> Option<UpdateFeaturesBrokerError> {
    let (code, message, message_truncated) = error.into_parts();
    Some(UpdateFeaturesBrokerError::new(
        NonZeroI16::new(code)?,
        message,
        message_truncated,
    ))
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

pub(super) const fn protocol_failure(error: UpdateFeaturesProtocolFailure) -> UpdateFeaturesInput {
    match error {
        UpdateFeaturesProtocolFailure::MissingSelectedVersion
        | UpdateFeaturesProtocolFailure::UnsupportedApiVersion { .. } => {
            UpdateFeaturesInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        UpdateFeaturesProtocolFailure::RetainedBytes { .. }
        | UpdateFeaturesProtocolFailure::Allocation { .. } => UpdateFeaturesInput::ResponseTooLarge,
        UpdateFeaturesProtocolFailure::NegativeThrottleTime { .. }
        | UpdateFeaturesProtocolFailure::TopLevelErrorWithResults
        | UpdateFeaturesProtocolFailure::SuccessDiagnostic { .. }
        | UpdateFeaturesProtocolFailure::V2ResultsPresent
        | UpdateFeaturesProtocolFailure::TooManyResults { .. }
        | UpdateFeaturesProtocolFailure::ResultCount { .. }
        | UpdateFeaturesProtocolFailure::EmptyFeatureName
        | UpdateFeaturesProtocolFailure::FeatureNameTooLong { .. }
        | UpdateFeaturesProtocolFailure::ResponseTextBytesExceeded { .. }
        | UpdateFeaturesProtocolFailure::UnexpectedFeature
        | UpdateFeaturesProtocolFailure::MissingFeature
        | UpdateFeaturesProtocolFailure::DuplicateFeature => UpdateFeaturesInput::InvalidResponse,
    }
}

const fn driver_failure(
    kind: UpdateFeaturesDriverFailureKind,
    delivery: DeliveryStatus,
) -> UpdateFeaturesInput {
    match kind {
        UpdateFeaturesDriverFailureKind::DeadlineElapsed => {
            UpdateFeaturesInput::DriverDeadlineElapsed { delivery }
        }
        UpdateFeaturesDriverFailureKind::Compatibility => {
            UpdateFeaturesInput::ProtocolIncompatible { delivery }
        }
        UpdateFeaturesDriverFailureKind::InvalidResponse => UpdateFeaturesInput::InvalidResponse,
        UpdateFeaturesDriverFailureKind::Transport => {
            UpdateFeaturesInput::TransportFailed { delivery }
        }
    }
}
