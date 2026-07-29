//! Fallible copying and request-order correlation for API-key 57 responses.

use kafka_wire::{UpdateFeaturesResponse, update_features_response::UpdatableFeatureResult};

use super::{
    NormalizedUpdateFeatureResult, NormalizedUpdateFeaturesError, NormalizedUpdateFeaturesOutcome,
    NormalizedUpdateFeaturesResponse, UpdateFeaturesProtocolFailure, UpdateFeaturesRequestPlan,
    retention::{
        bounded_diagnostic, ensure_limit, normalized_result_charge,
        normalized_top_level_error_charge,
    },
};

pub(super) fn materialize_top_level_error(
    throttle_time_ms: u32,
    response: &UpdateFeaturesResponse,
    source_required: usize,
    retained_limit: usize,
) -> Result<NormalizedUpdateFeaturesResponse, UpdateFeaturesProtocolFailure> {
    let error = normalized_error(
        response.error_code,
        response.error_message.as_deref(),
        retained_limit,
    )?;
    let normalized = normalized_top_level_error_charge(&error).unwrap_or(usize::MAX);
    ensure_limit(normalized, retained_limit)?;
    Ok(NormalizedUpdateFeaturesResponse::new(
        throttle_time_ms,
        NormalizedUpdateFeaturesOutcome::TopLevelError(error),
        source_required.max(normalized),
    ))
}

pub(super) fn materialize_results(
    selected_version: i16,
    throttle_time_ms: u32,
    response: &UpdateFeaturesResponse,
    plan: UpdateFeaturesRequestPlan<'_>,
    source_required: usize,
    retained_limit: usize,
) -> Result<NormalizedUpdateFeaturesResponse, UpdateFeaturesProtocolFailure> {
    let mut results = Vec::new();
    results
        .try_reserve_exact(plan.updates().len())
        .map_err(|_| UpdateFeaturesProtocolFailure::Allocation {
            field: "results",
            requested: plan.updates().len(),
        })?;
    for update in plan.updates() {
        let source = if selected_version <= 1 {
            Some(matching_result(update.feature(), &response.results)?)
        } else {
            None
        };
        let error = source
            .filter(|result| result.error_code != 0)
            .map(|result| {
                normalized_error(
                    result.error_code,
                    result.error_message.as_deref(),
                    retained_limit,
                )
            })
            .transpose()?;
        results.push(NormalizedUpdateFeatureResult::new(
            copy_text(update.feature(), "feature")?,
            error,
        ));
    }
    let normalized = normalized_result_charge(&results).unwrap_or(usize::MAX);
    ensure_limit(normalized, retained_limit)?;
    Ok(NormalizedUpdateFeaturesResponse::new(
        throttle_time_ms,
        NormalizedUpdateFeaturesOutcome::Results(results),
        source_required.max(normalized),
    ))
}

fn matching_result<'a>(
    feature: &str,
    results: &'a [UpdatableFeatureResult],
) -> Result<&'a UpdatableFeatureResult, UpdateFeaturesProtocolFailure> {
    results
        .iter()
        .find(|result| result.feature.as_str() == feature)
        .ok_or(UpdateFeaturesProtocolFailure::MissingFeature)
}

fn normalized_error(
    code: i16,
    source: Option<&str>,
    retained_limit: usize,
) -> Result<NormalizedUpdateFeaturesError, UpdateFeaturesProtocolFailure> {
    debug_assert_ne!(code, 0);
    let (bounded, truncated) = bounded_diagnostic(source);
    let message = bounded
        .map(|value| copy_text(value, "error_message"))
        .transpose()?;
    if message.as_ref().map_or(0, String::capacity) > retained_limit {
        return Err(UpdateFeaturesProtocolFailure::RetainedBytes {
            required: message.as_ref().map_or(0, String::capacity),
            limit: retained_limit,
        });
    }
    Ok(NormalizedUpdateFeaturesError::new(code, message, truncated))
}

fn copy_text(source: &str, field: &'static str) -> Result<String, UpdateFeaturesProtocolFailure> {
    let mut copied = String::new();
    copied.try_reserve_exact(source.len()).map_err(|_| {
        UpdateFeaturesProtocolFailure::Allocation {
            field,
            requested: source.len(),
        }
    })?;
    copied.push_str(source);
    Ok(copied)
}
