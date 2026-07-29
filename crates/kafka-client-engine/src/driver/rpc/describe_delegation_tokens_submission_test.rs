//! AnyBroker route, original deadline, Interactive lane, and v1-v3 window.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::describe_delegation_tokens_submission::{
    describe_delegation_tokens_options, describe_delegation_tokens_route,
};

#[test]
fn query_uses_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(describe_delegation_tokens_route(), Route::AnyBroker);
}

#[test]
fn query_preserves_original_deadline_and_exact_v1_v3_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = describe_delegation_tokens_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(3)));
}
