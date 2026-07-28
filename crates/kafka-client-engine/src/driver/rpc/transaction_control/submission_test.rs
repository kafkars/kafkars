//! Transaction-coordinator route and exact-v3 submission scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKind, Route, TrafficClass};

use super::submission::{transaction_control_options, transaction_control_route};

#[test]
fn route_uses_transaction_coordinator_authority() {
    let route = transaction_control_route("invoice-writer")
        .unwrap_or_else(|error| panic!("valid transaction route: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("transaction control must use coordinator routing");
    };
    assert_eq!(key.kind(), CoordinatorKind::Transaction);
    assert_eq!(key.as_str(), "invoice-writer");
}

#[test]
fn options_preserve_deadline_lane_and_exact_v3_bounds() {
    let deadline = Instant::now() + Duration::from_secs(2);
    let options = transaction_control_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(3)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(3)));
}
