//! Validate-first normalization of feature-bearing `ApiVersions` responses.

use kafka_wire::ApiVersionsResponse;

use super::{
    NormalizedDescribeFeaturesResponse,
    materialize::materialize_success,
    retention::{ensure_limit, error_charge, success_source_charge},
    validation::{MAX_VERSION, MIN_VERSION, validate_response},
};

/// Compatibility, hostile shape, allocation, scalar, or capacity failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeFeaturesProtocolFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    TooManyApiKeys {
        actual: usize,
        max: usize,
    },
    InvalidApiKey {
        actual: i16,
    },
    InvalidApiVersionRange {
        api_key: i16,
        min: i16,
        max: i16,
    },
    BrokerErrorWithFeaturePayload,
    TooManyFeatures {
        field: &'static str,
        actual: usize,
        max: usize,
    },
    EmptyFeatureName {
        field: &'static str,
    },
    FeatureNameTooLong {
        field: &'static str,
        actual: usize,
        max: usize,
    },
    FeatureTextBytesExceeded {
        required: usize,
        max: usize,
    },
    InvalidFeatureRange {
        field: &'static str,
        min: i16,
        max: i16,
    },
    InvalidFinalizedFeaturesEpoch {
        actual: i64,
    },
    FinalizedFeaturesWithoutEpoch,
    DuplicateFeatureName {
        field: &'static str,
    },
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Validates and copies one selected v3-v5 response without generated leakage.
pub(crate) fn normalize_describe_features_response(
    selected_version: Option<i16>,
    response: &ApiVersionsResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeFeaturesResponse, DescribeFeaturesProtocolFailure> {
    let selected_version =
        selected_version.ok_or(DescribeFeaturesProtocolFailure::MissingSelectedVersion)?;
    if !(MIN_VERSION..=MAX_VERSION).contains(&selected_version) {
        return Err(DescribeFeaturesProtocolFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        DescribeFeaturesProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let finalized_features_epoch = validate_response(response)?;
    let supported_features_complete = selected_version >= 4;
    if response.error_code != 0 {
        let required = error_charge();
        ensure_limit(required, retained_limit)?;
        return Ok(NormalizedDescribeFeaturesResponse::new(
            throttle_time_ms,
            response.error_code,
            Vec::new(),
            false,
            None,
            Vec::new(),
            false,
            required,
        ));
    }
    let required = success_source_charge(response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    materialize_success(
        throttle_time_ms,
        response,
        supported_features_complete,
        finalized_features_epoch,
        required,
        retained_limit,
    )
}
