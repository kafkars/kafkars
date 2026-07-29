//! Successful feature normalization and canonical-order evidence.

use kafka_wire::{
    ApiVersionsResponse,
    api_versions_response::{FinalizedFeatureKey, SupportedFeatureKey},
};

use super::{DESCRIBE_FEATURES_MAX_RETAINED_BYTES, normalize_describe_features_response};

fn supported(name: &str, min: i16, max: i16) -> SupportedFeatureKey {
    let mut feature = SupportedFeatureKey::default();
    feature.name = name.into();
    feature.min_version = min;
    feature.max_version = max;
    feature
}

fn finalized(name: &str, min: i16, max: i16) -> FinalizedFeatureKey {
    let mut feature = FinalizedFeatureKey::default();
    feature.name = name.into();
    feature.min_version_level = min;
    feature.max_version_level = max;
    feature
}

#[test]
fn v4_restores_utf8_name_order_and_preserves_complete_feature_facts() {
    let mut response = ApiVersionsResponse::default();
    response.throttle_time_ms = 17;
    response.supported_features = vec![
        supported("metadata.version", 0, 21),
        supported("kraft", 1, 2),
    ];
    response.finalized_features_epoch = 9;
    response.finalized_features = vec![
        finalized("metadata.version", 20, 20),
        finalized("kraft", 1, 1),
    ];
    response.zk_migration_ready = true;

    let normalized = normalize_describe_features_response(
        Some(4),
        &response,
        DESCRIBE_FEATURES_MAX_RETAINED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (throttle, code, supported, complete, epoch, finalized, migration, retained) =
        normalized.into_parts();

    assert_eq!(throttle, 17);
    assert_eq!(code, 0);
    assert!(complete);
    assert_eq!(epoch, Some(9));
    assert!(migration);
    assert!(retained > 0);
    assert_eq!(
        supported
            .into_iter()
            .map(|feature| feature.into_parts())
            .collect::<Vec<_>>(),
        vec![
            ("kraft".to_owned(), 1, 2),
            ("metadata.version".to_owned(), 0, 21),
        ]
    );
    assert_eq!(
        finalized
            .into_iter()
            .map(|feature| feature.into_parts())
            .collect::<Vec<_>>(),
        vec![
            ("kraft".to_owned(), 1, 1),
            ("metadata.version".to_owned(), 20, 20),
        ]
    );
}

#[test]
fn v5_preserves_unknown_epoch_when_no_finalized_features_exist() {
    let mut response = ApiVersionsResponse::default();
    response.supported_features = vec![supported("metadata.version", 0, 21)];

    let normalized = normalize_describe_features_response(
        Some(5),
        &response,
        DESCRIBE_FEATURES_MAX_RETAINED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (_, code, supported, complete, epoch, finalized, migration, _) = normalized.into_parts();

    assert_eq!(code, 0);
    assert_eq!(supported.len(), 1);
    assert!(complete);
    assert_eq!(epoch, None);
    assert!(finalized.is_empty());
    assert!(!migration);
}

#[test]
fn top_level_error_preserves_exact_signed_code_and_throttle() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = -32_000;
    response.throttle_time_ms = 23;

    let normalized = normalize_describe_features_response(
        Some(4),
        &response,
        DESCRIBE_FEATURES_MAX_RETAINED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid broker error: {error:?}"));
    let (throttle, code, supported, complete, epoch, finalized, migration, _) =
        normalized.into_parts();

    assert_eq!(throttle, 23);
    assert_eq!(code, -32_000);
    assert!(supported.is_empty());
    assert!(!complete);
    assert_eq!(epoch, None);
    assert!(finalized.is_empty());
    assert!(!migration);
}

#[test]
fn v3_marks_the_supported_feature_set_as_incomplete() {
    let mut response = ApiVersionsResponse::default();
    response.supported_features = vec![supported("metadata.version", 1, 21)];

    let normalized = normalize_describe_features_response(
        Some(3),
        &response,
        DESCRIBE_FEATURES_MAX_RETAINED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (_, code, supported, complete, _, _, _, _) = normalized.into_parts();

    assert_eq!(code, 0);
    assert_eq!(supported.len(), 1);
    assert!(!complete);
}
