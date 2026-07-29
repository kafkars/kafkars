//! Compatibility, hostile-shape, and retained-capacity rejection evidence.

use kafka_wire::{
    ApiVersionsResponse,
    api_versions_response::{FinalizedFeatureKey, SupportedFeatureKey},
};

use super::{
    DESCRIBE_FEATURES_MAX_RETAINED_BYTES, DescribeFeaturesProtocolFailure,
    normalize_describe_features_response,
};

fn supported(name: &str, min: i16, max: i16) -> SupportedFeatureKey {
    let mut feature = SupportedFeatureKey::default();
    feature.name = name.into();
    feature.min_version = min;
    feature.max_version = max;
    feature
}

#[test]
fn missing_or_pre_feature_selected_versions_are_rejected() {
    let response = ApiVersionsResponse::default();

    assert_eq!(
        normalize_describe_features_response(None, &response, DESCRIBE_FEATURES_MAX_RETAINED_BYTES),
        Err(DescribeFeaturesProtocolFailure::MissingSelectedVersion)
    );
    assert_eq!(
        normalize_describe_features_response(
            Some(2),
            &response,
            DESCRIBE_FEATURES_MAX_RETAINED_BYTES
        ),
        Err(DescribeFeaturesProtocolFailure::UnsupportedApiVersion { actual: 2 })
    );
}

#[test]
fn feature_ranges_and_names_must_be_unambiguous() {
    let mut response = ApiVersionsResponse::default();
    response.supported_features = vec![
        supported("metadata.version", 0, 21),
        supported("metadata.version", 0, 22),
    ];
    assert_eq!(
        normalize_describe_features_response(
            Some(4),
            &response,
            DESCRIBE_FEATURES_MAX_RETAINED_BYTES
        ),
        Err(DescribeFeaturesProtocolFailure::DuplicateFeatureName {
            field: "supported_features"
        })
    );

    response.supported_features = vec![supported("metadata.version", 2, 1)];
    assert_eq!(
        normalize_describe_features_response(
            Some(4),
            &response,
            DESCRIBE_FEATURES_MAX_RETAINED_BYTES
        ),
        Err(DescribeFeaturesProtocolFailure::InvalidFeatureRange {
            field: "supported_features",
            min: 2,
            max: 1,
        })
    );
}

#[test]
fn finalized_features_require_a_known_nonnegative_epoch() {
    let mut response = ApiVersionsResponse::default();
    let mut feature = FinalizedFeatureKey::default();
    feature.name = "metadata.version".into();
    feature.min_version_level = 20;
    feature.max_version_level = 20;
    response.finalized_features.push(feature);

    assert_eq!(
        normalize_describe_features_response(
            Some(4),
            &response,
            DESCRIBE_FEATURES_MAX_RETAINED_BYTES
        ),
        Err(DescribeFeaturesProtocolFailure::FinalizedFeaturesWithoutEpoch)
    );

    response.finalized_features.clear();
    response.finalized_features_epoch = -2;
    assert_eq!(
        normalize_describe_features_response(
            Some(4),
            &response,
            DESCRIBE_FEATURES_MAX_RETAINED_BYTES
        ),
        Err(DescribeFeaturesProtocolFailure::InvalidFinalizedFeaturesEpoch { actual: -2 })
    );
}

#[test]
fn negative_throttle_and_underreserved_terminals_are_rejected() {
    let mut response = ApiVersionsResponse::default();
    response.throttle_time_ms = -1;
    assert_eq!(
        normalize_describe_features_response(
            Some(4),
            &response,
            DESCRIBE_FEATURES_MAX_RETAINED_BYTES
        ),
        Err(DescribeFeaturesProtocolFailure::NegativeThrottleTime { actual: -1 })
    );

    response.throttle_time_ms = 0;
    response.supported_features = vec![supported("metadata.version", 0, 21)];
    assert!(matches!(
        normalize_describe_features_response(Some(4), &response, 1),
        Err(DescribeFeaturesProtocolFailure::RetainedBytes { limit: 1, .. })
    ));
}

#[test]
fn broker_errors_cannot_smuggle_success_feature_payloads() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 42;
    response.supported_features = vec![supported("metadata.version", 0, 21)];

    assert_eq!(
        normalize_describe_features_response(
            Some(4),
            &response,
            DESCRIBE_FEATURES_MAX_RETAINED_BYTES
        ),
        Err(DescribeFeaturesProtocolFailure::BrokerErrorWithFeaturePayload)
    );
}
