//! Engine request conversion and validation-boundary scenarios.

use super::{UpdateFeature, UpdateFeatureIntent, UpdateFeaturesRequest};

#[test]
fn request_preserves_order_intent_and_validation_mode() {
    let request = UpdateFeaturesRequest::new(
        vec![
            UpdateFeature::new(
                "metadata.version".to_owned(),
                7,
                UpdateFeatureIntent::Upgrade,
            ),
            UpdateFeature::new(
                "kraft.version".to_owned(),
                0,
                UpdateFeatureIntent::SafeDowngrade,
            ),
        ],
        true,
    );
    let plan = request
        .plan()
        .unwrap_or_else(|_| panic!("request should form a core plan"));
    assert_eq!(plan.updates()[0].feature(), "metadata.version");
    assert_eq!(plan.updates()[1].feature(), "kraft.version");
    assert!(plan.validate_only());
}

#[test]
fn invalid_inert_request_is_rejected_by_plan_conversion() {
    let request = UpdateFeaturesRequest::new(
        vec![UpdateFeature::new(
            "metadata.version".to_owned(),
            0,
            UpdateFeatureIntent::Upgrade,
        )],
        false,
    );
    assert!(request.plan().is_err());
}
