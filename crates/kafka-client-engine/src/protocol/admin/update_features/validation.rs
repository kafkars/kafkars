//! Allocation-free selected-version and hostile-shape validation.

use kafka_wire::UpdateFeaturesResponse;

use super::{
    UpdateFeaturesProtocolFailure, UpdateFeaturesRequestPlan, retention::bounded_diagnostic,
};

pub(super) const MAX_RESULTS: usize = 4 * 1024;
pub(super) const MAX_FEATURE_NAME_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_RESPONSE_TEXT_BYTES: usize = 1024 * 1024;

pub(super) fn validate_response(
    selected_version: i16,
    response: &UpdateFeaturesResponse,
    plan: UpdateFeaturesRequestPlan<'_>,
) -> Result<(), UpdateFeaturesProtocolFailure> {
    if response.error_code != 0 {
        return response
            .results
            .is_empty()
            .then_some(())
            .ok_or(UpdateFeaturesProtocolFailure::TopLevelErrorWithResults);
    }
    if response.error_message.is_some() {
        return Err(UpdateFeaturesProtocolFailure::SuccessDiagnostic {
            field: "top_level_error_message",
        });
    }
    if selected_version == 2 {
        return response
            .results
            .is_empty()
            .then_some(())
            .ok_or(UpdateFeaturesProtocolFailure::V2ResultsPresent);
    }
    if response.results.len() > MAX_RESULTS {
        return Err(UpdateFeaturesProtocolFailure::TooManyResults {
            actual: response.results.len(),
            max: MAX_RESULTS,
        });
    }
    if response.results.len() != plan.updates().len() {
        return Err(UpdateFeaturesProtocolFailure::ResultCount {
            expected: plan.updates().len(),
            actual: response.results.len(),
        });
    }
    validate_result_shapes(response, plan)
}

fn validate_result_shapes(
    response: &UpdateFeaturesResponse,
    plan: UpdateFeaturesRequestPlan<'_>,
) -> Result<(), UpdateFeaturesProtocolFailure> {
    let mut text_bytes = 0usize;
    for (index, result) in response.results.iter().enumerate() {
        if result.feature.is_empty() {
            return Err(UpdateFeaturesProtocolFailure::EmptyFeatureName);
        }
        if result.feature.len() > MAX_FEATURE_NAME_BYTES {
            return Err(UpdateFeaturesProtocolFailure::FeatureNameTooLong {
                actual: result.feature.len(),
                max: MAX_FEATURE_NAME_BYTES,
            });
        }
        if result.error_code == 0 && result.error_message.is_some() {
            return Err(UpdateFeaturesProtocolFailure::SuccessDiagnostic {
                field: "result_error_message",
            });
        }
        if !plan
            .updates()
            .iter()
            .any(|update| update.feature() == result.feature.as_str())
        {
            return Err(UpdateFeaturesProtocolFailure::UnexpectedFeature);
        }
        if response.results[..index]
            .iter()
            .any(|prior| prior.feature == result.feature)
        {
            return Err(UpdateFeaturesProtocolFailure::DuplicateFeature);
        }
        let diagnostic_bytes = bounded_diagnostic(result.error_message.as_deref())
            .0
            .map_or(0, str::len);
        text_bytes = text_bytes
            .checked_add(result.feature.len())
            .and_then(|bytes| bytes.checked_add(diagnostic_bytes))
            .unwrap_or(usize::MAX);
        if text_bytes > MAX_RESPONSE_TEXT_BYTES {
            return Err(UpdateFeaturesProtocolFailure::ResponseTextBytesExceeded {
                required: text_bytes,
                max: MAX_RESPONSE_TEXT_BYTES,
            });
        }
    }
    if plan.updates().iter().any(|update| {
        !response
            .results
            .iter()
            .any(|result| result.feature.as_str() == update.feature())
    }) {
        return Err(UpdateFeaturesProtocolFailure::MissingFeature);
    }
    Ok(())
}
