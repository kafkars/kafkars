//! Strict compatible-version normalization of API-key 57 responses.

use kafka_wire::UpdateFeaturesResponse;

use super::{
    NormalizedUpdateFeaturesResponse, UpdateFeaturesRequestPlan,
    materialize::{materialize_results, materialize_top_level_error},
    retention::{ensure_limit, result_source_charge, top_level_error_charge},
    validation::validate_response,
    version::supports_update_features_version,
};

/// Compatibility, hostile shape, allocation, scalar, correlation, or capacity failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpdateFeaturesProtocolFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    TopLevelErrorWithResults,
    SuccessDiagnostic {
        field: &'static str,
    },
    V2ResultsPresent,
    TooManyResults {
        actual: usize,
        max: usize,
    },
    ResultCount {
        expected: usize,
        actual: usize,
    },
    EmptyFeatureName,
    FeatureNameTooLong {
        actual: usize,
        max: usize,
    },
    ResponseTextBytesExceeded {
        required: usize,
        max: usize,
    },
    UnexpectedFeature,
    MissingFeature,
    DuplicateFeature,
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Validates, correlates, and copies one selected v0-v2 response.
pub(crate) fn normalize_update_features_response(
    selected_version: Option<i16>,
    response: &UpdateFeaturesResponse,
    plan: UpdateFeaturesRequestPlan<'_>,
    retained_limit: usize,
) -> Result<NormalizedUpdateFeaturesResponse, UpdateFeaturesProtocolFailure> {
    let selected_version =
        selected_version.ok_or(UpdateFeaturesProtocolFailure::MissingSelectedVersion)?;
    if !supports_update_features_version(selected_version) {
        return Err(UpdateFeaturesProtocolFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        UpdateFeaturesProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    validate_response(selected_version, response, plan)?;
    if response.error_code != 0 {
        let required = top_level_error_charge(response).unwrap_or(usize::MAX);
        ensure_limit(required, retained_limit)?;
        return materialize_top_level_error(throttle_time_ms, response, required, retained_limit);
    }
    let required = result_source_charge(selected_version, response, plan).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    materialize_results(
        selected_version,
        throttle_time_ms,
        response,
        plan,
        required,
        retained_limit,
    )
}
