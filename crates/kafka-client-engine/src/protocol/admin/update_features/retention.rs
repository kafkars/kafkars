//! Checked retained-capacity accounting and diagnostic bounding.

use core::mem::size_of;

use kafka_wire::UpdateFeaturesResponse;

use super::{
    NormalizedUpdateFeatureResult, NormalizedUpdateFeaturesError, NormalizedUpdateFeaturesOutcome,
    NormalizedUpdateFeaturesResponse, UpdateFeaturesProtocolFailure, UpdateFeaturesRequestPlan,
};

pub(super) const DIAGNOSTIC_BYTES: usize = 1024;

pub(super) fn top_level_error_charge(response: &UpdateFeaturesResponse) -> Option<usize> {
    size_of::<NormalizedUpdateFeaturesResponse>()
        .checked_add(size_of::<NormalizedUpdateFeaturesOutcome>())?
        .checked_add(size_of::<NormalizedUpdateFeaturesError>())?
        .checked_add(
            bounded_diagnostic(response.error_message.as_deref())
                .0
                .map_or(0, str::len),
        )
}

pub(super) fn result_source_charge(
    selected_version: i16,
    response: &UpdateFeaturesResponse,
    plan: UpdateFeaturesRequestPlan<'_>,
) -> Option<usize> {
    let owners = plan
        .updates()
        .len()
        .checked_mul(size_of::<NormalizedUpdateFeatureResult>())?;
    let names = plan.updates().iter().try_fold(0usize, |bytes, update| {
        bytes.checked_add(update.feature().len())
    })?;
    let diagnostics = if selected_version <= 1 {
        response.results.iter().try_fold(0usize, |bytes, result| {
            bytes.checked_add(
                bounded_diagnostic(result.error_message.as_deref())
                    .0
                    .map_or(0, str::len),
            )
        })?
    } else {
        0
    };
    size_of::<NormalizedUpdateFeaturesResponse>()
        .checked_add(size_of::<NormalizedUpdateFeaturesOutcome>())?
        .checked_add(owners)?
        .checked_add(names)?
        .checked_add(diagnostics)
}

pub(super) fn normalized_result_charge(results: &[NormalizedUpdateFeatureResult]) -> Option<usize> {
    let owners = results
        .len()
        .checked_mul(size_of::<NormalizedUpdateFeatureResult>())?;
    let text = results.iter().try_fold(0usize, |bytes, result| {
        bytes.checked_add(result.retained_text_bytes()?)
    })?;
    size_of::<NormalizedUpdateFeaturesResponse>()
        .checked_add(size_of::<NormalizedUpdateFeaturesOutcome>())?
        .checked_add(owners)?
        .checked_add(text)
}

pub(super) fn normalized_top_level_error_charge(
    error: &NormalizedUpdateFeaturesError,
) -> Option<usize> {
    size_of::<NormalizedUpdateFeaturesResponse>()
        .checked_add(size_of::<NormalizedUpdateFeaturesOutcome>())?
        .checked_add(size_of::<NormalizedUpdateFeaturesError>())?
        .checked_add(error.retained_message_bytes())
}

pub(super) fn bounded_diagnostic(source: Option<&str>) -> (Option<&str>, bool) {
    let Some(source) = source else {
        return (None, false);
    };
    if source.len() <= DIAGNOSTIC_BYTES {
        return (Some(source), false);
    }
    let mut end = DIAGNOSTIC_BYTES;
    while !source.is_char_boundary(end) {
        end -= 1;
    }
    (Some(&source[..end]), true)
}

pub(super) fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), UpdateFeaturesProtocolFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(UpdateFeaturesProtocolFailure::RetainedBytes { required, limit })
}
