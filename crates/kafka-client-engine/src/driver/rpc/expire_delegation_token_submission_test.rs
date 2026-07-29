//! AnyBroker route, original deadline, Interactive lane, and v1-v2 window.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::expire_delegation_token_submission::{
    expire_delegation_token_options, expire_delegation_token_route,
};

#[test]
fn expiration_uses_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(expire_delegation_token_route(), Route::AnyBroker);
}

#[test]
fn expiration_preserves_original_deadline_and_exact_v1_v2_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = expire_delegation_token_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}
