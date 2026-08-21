//! Bounded finalized-feature request validation tests.

use super::{
    FeatureUpdate, UpdateFeaturesRequest, UpdateFeaturesRequestError,
    request::{MAX_FEATURE_NAME_BYTES, MAX_FEATURE_TEXT_BYTES, MAX_UPDATES},
};

#[test]
fn valid_request_preserves_order_and_validate_only_intent() {
    let request = UpdateFeaturesRequest::new(vec![
        FeatureUpdate::upgrade("metadata.version", 12),
        FeatureUpdate::safe_downgrade("transaction.version", 4),
        FeatureUpdate::unsafe_downgrade("group.version", 0),
    ])
    .with_validate_only(true);

    assert_eq!(request.validate(), Ok(()));

    let (updates, validate_only) = request.into_parts();
    assert!(validate_only);
    assert_eq!(updates[0].feature_name(), "metadata.version");
    assert_eq!(updates[1].feature_name(), "transaction.version");
    assert_eq!(updates[2].feature_name(), "group.version");
}

#[test]
fn empty_duplicate_and_count_bounds_are_rejected() {
    assert_eq!(
        UpdateFeaturesRequest::new(Vec::new()).validate(),
        Err(UpdateFeaturesRequestError::EmptyBatch)
    );
    assert_eq!(
        UpdateFeaturesRequest::new(vec![
            FeatureUpdate::upgrade("metadata.version", 11),
            FeatureUpdate::safe_downgrade("metadata.version", 10),
        ])
        .validate(),
        Err(UpdateFeaturesRequestError::DuplicateFeature)
    );

    let updates = (0..=MAX_UPDATES)
        .map(|index| FeatureUpdate::upgrade(format!("feature-{index}"), 1))
        .collect();
    assert_eq!(
        UpdateFeaturesRequest::new(updates).validate(),
        Err(UpdateFeaturesRequestError::TooManyUpdates)
    );
}

#[test]
fn names_and_aggregate_text_are_bounded() {
    assert_eq!(
        UpdateFeaturesRequest::new(vec![FeatureUpdate::upgrade("", 1)]).validate(),
        Err(UpdateFeaturesRequestError::EmptyFeatureName)
    );
    assert_eq!(
        UpdateFeaturesRequest::new(vec![FeatureUpdate::upgrade(
            "x".repeat(MAX_FEATURE_NAME_BYTES + 1),
            1,
        )])
        .validate(),
        Err(UpdateFeaturesRequestError::FeatureNameTooLong)
    );

    let name_count = MAX_FEATURE_TEXT_BYTES / MAX_FEATURE_NAME_BYTES + 1;
    let updates = (0..name_count)
        .map(|index| {
            let suffix = format!("{index:04}");
            let mut name = "x".repeat(MAX_FEATURE_NAME_BYTES - suffix.len());
            name.push_str(&suffix);
            FeatureUpdate::upgrade(name, 1)
        })
        .collect();
    assert_eq!(
        UpdateFeaturesRequest::new(updates).validate(),
        Err(UpdateFeaturesRequestError::FeatureTextBytesExceeded)
    );
}

#[test]
fn level_zero_is_only_a_downgrade_and_negative_levels_are_rejected() {
    assert_eq!(
        UpdateFeaturesRequest::new(vec![FeatureUpdate::upgrade("metadata.version", 0)]).validate(),
        Err(UpdateFeaturesRequestError::DeletionRequiresDowngrade)
    );
    assert_eq!(
        UpdateFeaturesRequest::new(vec![FeatureUpdate::safe_downgrade("metadata.version", -1,)])
            .validate(),
        Err(UpdateFeaturesRequestError::NegativeVersionLevel)
    );
    assert_eq!(
        UpdateFeaturesRequest::new(vec![FeatureUpdate::safe_downgrade("metadata.version", 0,)])
            .validate(),
        Ok(())
    );
    assert_eq!(
        UpdateFeaturesRequest::new(vec![
            FeatureUpdate::unsafe_downgrade("metadata.version", 0,)
        ])
        .validate(),
        Ok(())
    );
}
