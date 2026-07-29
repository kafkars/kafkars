//! AnyBroker route, original deadline, lane, and exact-v0 submission evidence.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::unregister_broker_submission::{unregister_broker_options, unregister_broker_route};

#[test]
fn mutation_uses_any_broker_and_preserves_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let options = unregister_broker_options(deadline);

    assert_eq!(unregister_broker_route(), Route::AnyBroker);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}
