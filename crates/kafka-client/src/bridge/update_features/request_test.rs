//! Public-to-engine finalized-feature request translation tests.

use crate::{
    DeliveryStatus, ErrorKind,
    admin::{FeatureUpdate, UpdateFeaturesRequest},
};

use super::{
    engine::FeatureIntent,
    request::{translate_intent, translate_request},
};

#[test]
fn explicit_intents_order_levels_and_validate_only_translate_exactly() {
    let request = UpdateFeaturesRequest::new(vec![
        FeatureUpdate::upgrade("metadata.version", 12),
        FeatureUpdate::safe_downgrade("transaction.version", 4),
        FeatureUpdate::unsafe_downgrade("group.version", 0),
    ])
    .with_validate_only(true);

    let translated =
        translate_request(request).unwrap_or_else(|error| panic!("valid translation: {error}"));
    assert!(translated.validate_only());
    assert_eq!(translated.updates()[0].feature(), "metadata.version");
    assert_eq!(translated.updates()[0].max_version_level(), 12);
    assert_eq!(translated.updates()[0].intent(), FeatureIntent::Upgrade);
    assert_eq!(
        translated.updates()[1].intent(),
        FeatureIntent::SafeDowngrade
    );
    assert_eq!(translated.updates()[2].max_version_level(), 0);
    assert_eq!(
        translated.updates()[2].intent(),
        FeatureIntent::UnsafeDowngrade
    );
}

#[test]
fn every_public_intent_has_one_exact_engine_mapping() {
    assert_eq!(
        translate_intent(crate::admin::FeatureUpdateIntent::Upgrade),
        FeatureIntent::Upgrade
    );
    assert_eq!(
        translate_intent(crate::admin::FeatureUpdateIntent::SafeDowngrade),
        FeatureIntent::SafeDowngrade
    );
    assert_eq!(
        translate_intent(crate::admin::FeatureUpdateIntent::UnsafeDowngrade),
        FeatureIntent::UnsafeDowngrade
    );
}

#[test]
fn malformed_request_is_definitely_unsent_configuration_failure() {
    let error = translate_request(UpdateFeaturesRequest::new(vec![FeatureUpdate::upgrade(
        "metadata.version",
        0,
    )]))
    .err()
    .unwrap_or_else(|| panic!("zero-level upgrade must fail"));

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
