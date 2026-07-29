//! API-key, feature-floor, request-shape, bound, and capacity evidence.

use kafka_wire::{KafkaMessage, KafkaRequest, UpdateFeaturesRequest};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use super::{
    PreparedUpdateFeaturesRequest, UPDATE_FEATURES_MAX_VERSION, UpdateFeatureMode,
    UpdateFeatureRef, UpdateFeaturesRequestFailure, UpdateFeaturesRequestPlan,
    request::{MAX_FEATURE_NAME_BYTES, MAX_UPDATES},
    update_features_request,
};

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn generated_contract_is_flexible_api_57_v0_through_v2() {
    assert_eq!(<UpdateFeaturesRequest as KafkaRequest>::API_KEY.value(), 57);
    for version in 0..=UPDATE_FEATURES_MAX_VERSION {
        let version = ApiVersion::new(version);
        assert!(UpdateFeaturesRequest::SUPPORTED_VERSIONS.contains(version));
        assert!(UpdateFeaturesRequest::is_flexible(version));
    }
    assert!(!UpdateFeaturesRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(3)));
}

#[test]
fn apply_only_upgrade_and_safe_downgrade_keep_the_v0_floor() {
    let updates = [
        update("metadata.version", 21, UpdateFeatureMode::Upgrade),
        update("group.version", 1, UpdateFeatureMode::SafeDowngrade),
    ];
    let (request, floor) = prepared(&updates, false);
    let legacy = request
        .request_for_test(0)
        .unwrap_or_else(|| panic!("v0 representation"));
    let modern = request
        .request_for_test(2)
        .unwrap_or_else(|| panic!("modern representation"));

    assert_eq!(floor, 0);
    assert_eq!(legacy.timeout_ms, 7000);
    assert!(!legacy.validate_only);
    assert_eq!(
        legacy.feature_updates[0].feature.as_str(),
        "metadata.version"
    );
    assert!(!legacy.feature_updates[0].allow_downgrade);
    assert!(legacy.feature_updates[1].allow_downgrade);
    assert!(!modern.feature_updates[1].allow_downgrade);
    assert_eq!(modern.feature_updates[1].upgrade_type, 2);
    assert!(request.encoded_len(ApiVersion::new(0)).is_ok());
    assert!(request.encoded_len(ApiVersion::new(2)).is_ok());
}

#[test]
fn validate_only_or_unsafe_downgrade_raises_the_floor_to_v1() {
    let upgrade = [update("group.version", 1, UpdateFeatureMode::Upgrade)];
    let (validated, validate_floor) = prepared(&upgrade, true);
    assert_eq!(validate_floor, 1);
    assert!(validated.encoded_len(ApiVersion::new(0)).is_err());
    assert!(validated.encoded_len(ApiVersion::new(1)).is_ok());

    let unsafe_updates = [update(
        "metadata.version",
        20,
        UpdateFeatureMode::UnsafeDowngrade,
    )];
    let (unsafe_request, unsafe_floor) = prepared(&unsafe_updates, false);
    let modern = unsafe_request
        .request_for_test(1)
        .unwrap_or_else(|| panic!("modern representation"));
    assert_eq!(unsafe_floor, 1);
    assert_eq!(modern.feature_updates[0].upgrade_type, 3);
    assert!(unsafe_request.encoded_len(ApiVersion::new(0)).is_err());
    assert!(unsafe_request.encoded_len(ApiVersion::new(2)).is_ok());
}

#[test]
fn invalid_scalar_duplicate_count_and_capacity_shapes_are_rejected() {
    assert_failure(&[], false, UpdateFeaturesRequestFailure::EmptyUpdates);
    assert_failure(
        &[update("", 1, UpdateFeatureMode::Upgrade)],
        false,
        UpdateFeaturesRequestFailure::EmptyFeatureName,
    );
    assert_failure(
        &[
            update("same", 1, UpdateFeatureMode::Upgrade),
            update("same", 2, UpdateFeatureMode::Upgrade),
        ],
        false,
        UpdateFeaturesRequestFailure::DuplicateFeature,
    );
    assert_failure(
        &[update("feature", -1, UpdateFeatureMode::Upgrade)],
        false,
        UpdateFeaturesRequestFailure::NegativeMaxVersionLevel { actual: -1 },
    );
    assert_failure(
        &[update("feature", 0, UpdateFeatureMode::Upgrade)],
        false,
        UpdateFeaturesRequestFailure::DeletionRequiresDowngrade,
    );

    let too_many = vec![update("x", 1, UpdateFeatureMode::Upgrade); MAX_UPDATES + 1];
    assert!(matches!(
        build(&too_many, false, 7000, LIMIT),
        Err(UpdateFeaturesRequestFailure::TooManyUpdates { .. })
    ));
    let long = "x".repeat(MAX_FEATURE_NAME_BYTES + 1);
    assert!(matches!(
        build(
            &[update(&long, 1, UpdateFeatureMode::Upgrade)],
            false,
            7000,
            LIMIT,
        ),
        Err(UpdateFeaturesRequestFailure::FeatureNameTooLong { .. })
    ));
    assert_eq!(
        build(
            &[update("feature", 1, UpdateFeatureMode::Upgrade)],
            false,
            -1,
            LIMIT,
        )
        .err(),
        Some(UpdateFeaturesRequestFailure::NegativeTimeout { actual: -1 })
    );
    assert!(matches!(
        build(
            &[update("feature", 1, UpdateFeatureMode::Upgrade)],
            false,
            7000,
            0,
        ),
        Err(UpdateFeaturesRequestFailure::RetainedBytes { .. })
    ));
}

fn update(feature: &str, level: i16, mode: UpdateFeatureMode) -> UpdateFeatureRef<'_> {
    UpdateFeatureRef::new(feature, level, mode)
}

fn prepared(
    updates: &[UpdateFeatureRef<'_>],
    validate_only: bool,
) -> (PreparedUpdateFeaturesRequest, i16) {
    build(updates, validate_only, 7000, LIMIT)
        .unwrap_or_else(|error| panic!("valid request: {error:?}"))
}

fn build(
    updates: &[UpdateFeatureRef<'_>],
    validate_only: bool,
    timeout_ms: i32,
    limit: usize,
) -> Result<(PreparedUpdateFeaturesRequest, i16), UpdateFeaturesRequestFailure> {
    update_features_request(
        UpdateFeaturesRequestPlan::new(updates, validate_only),
        timeout_ms,
        limit,
    )
}

fn assert_failure(
    updates: &[UpdateFeatureRef<'_>],
    validate_only: bool,
    expected: UpdateFeaturesRequestFailure,
) {
    assert_eq!(
        build(updates, validate_only, 7000, LIMIT).err(),
        Some(expected)
    );
}
