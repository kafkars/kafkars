//! Allocation-free hostile-shape validation for API-key 18 v3-v5.

use kafka_wire::ApiVersionsResponse;

use super::DescribeFeaturesProtocolFailure;

pub(crate) const DESCRIBE_FEATURES_MAX_FEATURES: usize = 1_024;
pub(crate) const DESCRIBE_FEATURES_MAX_FEATURE_NAME_BYTES: usize = 256;
pub(crate) const DESCRIBE_FEATURES_MAX_FEATURE_TEXT_BYTES: usize = 256 * 1_024;
#[cfg(test)]
pub(crate) const DESCRIBE_FEATURES_MAX_RETAINED_BYTES: usize = 1_024 * 1_024;
pub(super) const MIN_VERSION: i16 = 3;
pub(super) const MAX_VERSION: i16 = 5;
const MAX_API_KEYS: usize = 1_024;

pub(super) fn validate_response(
    response: &ApiVersionsResponse,
) -> Result<Option<i64>, DescribeFeaturesProtocolFailure> {
    validate_api_keys(response)?;
    if response.error_code != 0 {
        let has_feature_payload = !response.supported_features.is_empty()
            || response.finalized_features_epoch != -1
            || !response.finalized_features.is_empty()
            || response.zk_migration_ready;
        return (!has_feature_payload)
            .then_some(None)
            .ok_or(DescribeFeaturesProtocolFailure::BrokerErrorWithFeaturePayload);
    }
    validate_feature_counts(response)?;
    let mut text_bytes = 0usize;
    for feature in &response.supported_features {
        validate_name("supported_features", feature.name.as_str(), &mut text_bytes)?;
        validate_range(
            "supported_features",
            feature.min_version,
            feature.max_version,
        )?;
    }
    for feature in &response.finalized_features {
        validate_name("finalized_features", feature.name.as_str(), &mut text_bytes)?;
        validate_range(
            "finalized_features",
            feature.min_version_level,
            feature.max_version_level,
        )?;
    }
    match response.finalized_features_epoch {
        -1 if response.finalized_features.is_empty() => Ok(None),
        -1 => Err(DescribeFeaturesProtocolFailure::FinalizedFeaturesWithoutEpoch),
        epoch if epoch >= 0 => Ok(Some(epoch)),
        actual => Err(DescribeFeaturesProtocolFailure::InvalidFinalizedFeaturesEpoch { actual }),
    }
}

fn validate_api_keys(
    response: &ApiVersionsResponse,
) -> Result<(), DescribeFeaturesProtocolFailure> {
    if response.api_keys.len() > MAX_API_KEYS {
        return Err(DescribeFeaturesProtocolFailure::TooManyApiKeys {
            actual: response.api_keys.len(),
            max: MAX_API_KEYS,
        });
    }
    for api in &response.api_keys {
        if api.api_key < 0 {
            return Err(DescribeFeaturesProtocolFailure::InvalidApiKey {
                actual: api.api_key,
            });
        }
        if api.min_version < 0 || api.max_version < api.min_version {
            return Err(DescribeFeaturesProtocolFailure::InvalidApiVersionRange {
                api_key: api.api_key,
                min: api.min_version,
                max: api.max_version,
            });
        }
    }
    Ok(())
}

fn validate_feature_counts(
    response: &ApiVersionsResponse,
) -> Result<(), DescribeFeaturesProtocolFailure> {
    for (field, actual) in [
        ("supported_features", response.supported_features.len()),
        ("finalized_features", response.finalized_features.len()),
    ] {
        if actual > DESCRIBE_FEATURES_MAX_FEATURES {
            return Err(DescribeFeaturesProtocolFailure::TooManyFeatures {
                field,
                actual,
                max: DESCRIBE_FEATURES_MAX_FEATURES,
            });
        }
    }
    Ok(())
}

fn validate_name(
    field: &'static str,
    name: &str,
    text_bytes: &mut usize,
) -> Result<(), DescribeFeaturesProtocolFailure> {
    if name.is_empty() {
        return Err(DescribeFeaturesProtocolFailure::EmptyFeatureName { field });
    }
    if name.len() > DESCRIBE_FEATURES_MAX_FEATURE_NAME_BYTES {
        return Err(DescribeFeaturesProtocolFailure::FeatureNameTooLong {
            field,
            actual: name.len(),
            max: DESCRIBE_FEATURES_MAX_FEATURE_NAME_BYTES,
        });
    }
    *text_bytes = text_bytes.checked_add(name.len()).unwrap_or(usize::MAX);
    if *text_bytes > DESCRIBE_FEATURES_MAX_FEATURE_TEXT_BYTES {
        return Err(DescribeFeaturesProtocolFailure::FeatureTextBytesExceeded {
            required: *text_bytes,
            max: DESCRIBE_FEATURES_MAX_FEATURE_TEXT_BYTES,
        });
    }
    Ok(())
}

fn validate_range(
    field: &'static str,
    min: i16,
    max: i16,
) -> Result<(), DescribeFeaturesProtocolFailure> {
    (min >= 0 && max >= min)
        .then_some(())
        .ok_or(DescribeFeaturesProtocolFailure::InvalidFeatureRange { field, min, max })
}
