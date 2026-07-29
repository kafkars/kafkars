//! AnyBroker route, original deadline, Interactive lane, and owner-aware floor.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::create_delegation_token_submission::{
    create_delegation_token_minimum_version, create_delegation_token_options,
    create_delegation_token_route,
};

#[test]
fn route_is_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(create_delegation_token_route(), Route::AnyBroker);
}

#[test]
fn default_owner_preserves_the_v1_through_v3_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let minimum = create_delegation_token_minimum_version(1);
    let options = create_delegation_token_options(deadline, minimum);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(3)));
}

#[test]
fn explicit_owner_raises_the_floor_to_v3_without_a_retry_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let minimum = create_delegation_token_minimum_version(3);
    let options = create_delegation_token_options(deadline, minimum);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(3)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(3)));
}
