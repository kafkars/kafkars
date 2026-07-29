//! Feature-metadata canonicalization, compatibility, and bound scenarios.

use super::{
    DESCRIBE_FEATURES_MAX_FEATURE_NAME_BYTES, DESCRIBE_FEATURES_MAX_FEATURE_TEXT_BYTES,
    DESCRIBE_FEATURES_MAX_FEATURES_PER_COLLECTION, DESCRIBE_FEATURES_MAX_RETAINED_BYTES,
    DescribeFeaturesDescription, DescribeFeaturesFinalizedFeature,
    DescribeFeaturesSupportedFeature, DescribeFeaturesValueError,
};

#[test]
fn description_canonicalizes_both_maps_and_preserves_version_completeness() {
    let description = DescribeFeaturesDescription::new(
        19,
        vec![supported("zeta", 1, 4), supported("alpha", 0, 2)],
        false,
        Some(42),
        vec![finalized("zeta", 2, 3), finalized("alpha", 1, 1)],
        true,
    )
    .unwrap_or_else(|error| panic!("description: {error}"));

    assert_eq!(description.throttle_time_ms(), 19);
    assert_eq!(names(description.supported_features()), ["alpha", "zeta"]);
    assert!(!description.supported_features_complete());
    assert_eq!(description.finalized_features_epoch(), Some(42));
    assert_eq!(
        finalized_names(description.finalized_features()),
        ["alpha", "zeta"]
    );
    assert!(description.zk_migration_ready());

    let (throttle, supported, complete, epoch, finalized, migration_ready) =
        description.into_parts();
    assert_eq!(throttle, 19);
    assert_eq!(names(&supported), ["alpha", "zeta"]);
    assert!(!complete);
    assert_eq!(epoch, Some(42));
    assert_eq!(finalized_names(&finalized), ["alpha", "zeta"]);
    assert!(migration_ready);
}

#[test]
fn v4_or_newer_can_report_a_complete_empty_feature_set() {
    let description = DescribeFeaturesDescription::new(0, vec![], true, None, vec![], false)
        .unwrap_or_else(|error| panic!("description: {error}"));
    assert!(description.supported_features_complete());
    assert!(description.supported_features().is_empty());
    assert_eq!(description.finalized_features_epoch(), None);
}

#[test]
fn names_ranges_and_epoch_relationship_are_validated() {
    assert_error(
        vec![supported("", 0, 1)],
        true,
        None,
        vec![],
        DescribeFeaturesValueError::EmptySupportedFeatureName,
    );
    assert_error(
        vec![supported(
            &"x".repeat(DESCRIBE_FEATURES_MAX_FEATURE_NAME_BYTES + 1),
            0,
            1,
        )],
        true,
        None,
        vec![],
        DescribeFeaturesValueError::SupportedFeatureNameTooLong,
    );
    assert_error(
        vec![supported("range", -1, 1)],
        true,
        None,
        vec![],
        DescribeFeaturesValueError::InvalidSupportedFeatureRange,
    );
    assert_error(
        vec![],
        true,
        Some(-2),
        vec![],
        DescribeFeaturesValueError::NegativeFinalizedFeaturesEpoch,
    );
    assert_error(
        vec![],
        true,
        None,
        vec![finalized("orphan", 0, 1)],
        DescribeFeaturesValueError::FinalizedFeaturesWithoutEpoch,
    );
    assert_error(
        vec![],
        true,
        Some(1),
        vec![finalized("range", 2, 1)],
        DescribeFeaturesValueError::InvalidFinalizedFeatureRange,
    );
}

#[test]
fn duplicate_names_are_rejected_after_canonical_sorting() {
    assert_error(
        vec![supported("same", 0, 1), supported("same", 0, 2)],
        true,
        None,
        vec![],
        DescribeFeaturesValueError::DuplicateSupportedFeature,
    );
    assert_error(
        vec![],
        true,
        Some(1),
        vec![finalized("same", 0, 1), finalized("same", 1, 1)],
        DescribeFeaturesValueError::DuplicateFinalizedFeature,
    );
}

#[test]
fn count_text_and_concrete_retention_are_bounded_independently() {
    let too_many = (0..=DESCRIBE_FEATURES_MAX_FEATURES_PER_COLLECTION)
        .map(|index| supported(&format!("f{index}"), 0, 1))
        .collect();
    assert_error(
        too_many,
        true,
        None,
        vec![],
        DescribeFeaturesValueError::TooManySupportedFeatures,
    );

    let text_limit_entries =
        DESCRIBE_FEATURES_MAX_FEATURE_TEXT_BYTES / DESCRIBE_FEATURES_MAX_FEATURE_NAME_BYTES;
    let full_names = (0..text_limit_entries)
        .map(|index| {
            let prefix = format!("f{index:04}");
            let padding = "x".repeat(DESCRIBE_FEATURES_MAX_FEATURE_NAME_BYTES - prefix.len());
            supported(&(prefix + &padding), 0, 1)
        })
        .collect();
    assert_error(
        full_names,
        true,
        Some(1),
        vec![finalized("extra", 0, 1)],
        DescribeFeaturesValueError::FeatureTextBytesExceeded,
    );

    let mut retained = Vec::with_capacity(
        DESCRIBE_FEATURES_MAX_RETAINED_BYTES
            / core::mem::size_of::<DescribeFeaturesSupportedFeature>()
            + 1,
    );
    retained.push(supported("small", 0, 1));
    assert_error(
        retained,
        true,
        None,
        vec![],
        DescribeFeaturesValueError::RetainedBytesExceeded,
    );
}

fn assert_error(
    supported: Vec<DescribeFeaturesSupportedFeature>,
    complete: bool,
    epoch: Option<i64>,
    finalized: Vec<DescribeFeaturesFinalizedFeature>,
    expected: DescribeFeaturesValueError,
) {
    assert_eq!(
        DescribeFeaturesDescription::new(0, supported, complete, epoch, finalized, false),
        Err(expected)
    );
}

fn supported(name: &str, min: i16, max: i16) -> DescribeFeaturesSupportedFeature {
    DescribeFeaturesSupportedFeature::new(name.to_owned(), min, max)
}

fn finalized(name: &str, min: i16, max: i16) -> DescribeFeaturesFinalizedFeature {
    DescribeFeaturesFinalizedFeature::new(name.to_owned(), min, max)
}

fn names(features: &[DescribeFeaturesSupportedFeature]) -> Vec<&str> {
    features
        .iter()
        .map(DescribeFeaturesSupportedFeature::name)
        .collect()
}

fn finalized_names(features: &[DescribeFeaturesFinalizedFeature]) -> Vec<&str> {
    features
        .iter()
        .map(DescribeFeaturesFinalizedFeature::name)
        .collect()
}
