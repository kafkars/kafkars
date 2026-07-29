//! Bounds, uniqueness, deletion, and explicit-intent plan scenarios.

use super::{
    UPDATE_FEATURES_MAX_FEATURE_NAME_BYTES, UPDATE_FEATURES_MAX_FEATURE_TEXT_BYTES,
    UPDATE_FEATURES_MAX_UPDATES, UpdateFeature, UpdateFeatureIntent, UpdateFeaturesPlan,
    UpdateFeaturesPlanError,
};

#[test]
fn plan_retains_caller_order_intents_and_validate_only() {
    let plan = UpdateFeaturesPlan::new(
        vec![
            UpdateFeature::new(
                "metadata.version".to_owned(),
                19,
                UpdateFeatureIntent::Upgrade,
            ),
            UpdateFeature::new(
                "kraft.version".to_owned(),
                1,
                UpdateFeatureIntent::SafeDowngrade,
            ),
            UpdateFeature::new(
                "legacy.feature".to_owned(),
                0,
                UpdateFeatureIntent::UnsafeDowngrade,
            ),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert!(plan.validate_only());
    assert_eq!(plan.updates()[0].feature(), "metadata.version");
    assert_eq!(
        plan.updates()[1].intent(),
        UpdateFeatureIntent::SafeDowngrade
    );
    assert!(plan.updates()[2].is_deletion());
    let (updates, validate_only) = plan.into_parts();
    assert!(validate_only);
    assert_eq!(
        updates[2].clone().into_parts(),
        (
            "legacy.feature".to_owned(),
            0,
            UpdateFeatureIntent::UnsafeDowngrade,
        )
    );
}

#[test]
fn level_zero_requires_explicit_downgrade_but_both_policies_may_delete() {
    assert_eq!(
        UpdateFeaturesPlan::new(
            vec![UpdateFeature::new(
                "feature".to_owned(),
                0,
                UpdateFeatureIntent::Upgrade,
            )],
            false,
        ),
        Err(UpdateFeaturesPlanError::DeletionRequiresDowngrade)
    );
    for intent in [
        UpdateFeatureIntent::SafeDowngrade,
        UpdateFeatureIntent::UnsafeDowngrade,
    ] {
        UpdateFeaturesPlan::new(
            vec![UpdateFeature::new("feature".to_owned(), 0, intent)],
            false,
        )
        .unwrap_or_else(|error| panic!("explicit downgrade may delete: {error}"));
    }
}

#[test]
fn positive_levels_accept_every_explicit_direction_and_loss_policy() {
    for intent in [
        UpdateFeatureIntent::Upgrade,
        UpdateFeatureIntent::SafeDowngrade,
        UpdateFeatureIntent::UnsafeDowngrade,
    ] {
        let plan = UpdateFeaturesPlan::new(
            vec![UpdateFeature::new("feature".to_owned(), 1, intent)],
            false,
        )
        .unwrap_or_else(|error| panic!("positive target should be valid: {error}"));
        assert_eq!(plan.updates()[0].max_version_level(), 1);
    }
}

#[test]
fn empty_duplicate_negative_and_overlong_updates_are_rejected() {
    assert_eq!(
        UpdateFeaturesPlan::new(Vec::new(), false),
        Err(UpdateFeaturesPlanError::EmptyBatch)
    );
    assert_eq!(
        UpdateFeaturesPlan::new(
            vec![UpdateFeature::new(
                String::new(),
                1,
                UpdateFeatureIntent::Upgrade,
            )],
            false,
        ),
        Err(UpdateFeaturesPlanError::EmptyFeatureName)
    );
    assert_eq!(
        UpdateFeaturesPlan::new(
            vec![
                UpdateFeature::new("same".to_owned(), 1, UpdateFeatureIntent::Upgrade),
                UpdateFeature::new("same".to_owned(), 2, UpdateFeatureIntent::Upgrade),
            ],
            false,
        ),
        Err(UpdateFeaturesPlanError::DuplicateFeature)
    );
    assert_eq!(
        UpdateFeaturesPlan::new(
            vec![UpdateFeature::new(
                "feature".to_owned(),
                -1,
                UpdateFeatureIntent::SafeDowngrade,
            )],
            false,
        ),
        Err(UpdateFeaturesPlanError::NegativeVersionLevel)
    );
    assert_eq!(
        UpdateFeaturesPlan::new(
            vec![UpdateFeature::new(
                "x".repeat(UPDATE_FEATURES_MAX_FEATURE_NAME_BYTES + 1),
                1,
                UpdateFeatureIntent::Upgrade,
            )],
            false,
        ),
        Err(UpdateFeaturesPlanError::FeatureNameTooLong)
    );
}

#[test]
fn feature_count_and_aggregate_text_bounds_are_independent() {
    assert_eq!(
        UpdateFeaturesPlan::new(
            vec![
                UpdateFeature::new("same".to_owned(), 1, UpdateFeatureIntent::Upgrade);
                UPDATE_FEATURES_MAX_UPDATES + 1
            ],
            false,
        ),
        Err(UpdateFeaturesPlanError::TooManyUpdates)
    );

    let updates = (0..=(UPDATE_FEATURES_MAX_FEATURE_TEXT_BYTES
        / UPDATE_FEATURES_MAX_FEATURE_NAME_BYTES))
        .map(|index| {
            let prefix = format!("{index:04}-");
            let feature = format!(
                "{prefix}{}",
                "x".repeat(UPDATE_FEATURES_MAX_FEATURE_NAME_BYTES - prefix.len())
            );
            UpdateFeature::new(feature, 1, UpdateFeatureIntent::Upgrade)
        })
        .collect();
    assert_eq!(
        UpdateFeaturesPlan::new(updates, false),
        Err(UpdateFeaturesPlanError::FeatureTextBytesExceeded)
    );
}
