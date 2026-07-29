//! Route, deadline, traffic, and conditional-version submission evidence.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::update_features_submission::{
    UpdateFeaturesSubmitError, update_features_options, update_features_route,
};

#[test]
fn mutation_uses_the_controller_and_preserves_the_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = update_features_options(deadline, 0)
        .unwrap_or_else(|error| panic!("valid options: {error}"));

    assert_eq!(update_features_route(), Route::Controller);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}

#[test]
fn feature_policy_can_raise_only_the_minimum_to_v1() {
    let options = update_features_options(Instant::now(), 1)
        .unwrap_or_else(|error| panic!("valid options: {error}"));

    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
    assert!(matches!(
        update_features_options(Instant::now(), -1),
        Err(UpdateFeaturesSubmitError::InvalidVersionFloor { actual: -1 })
    ));
    assert!(matches!(
        update_features_options(Instant::now(), 2),
        Err(UpdateFeaturesSubmitError::InvalidVersionFloor { actual: 2 })
    ));
}
