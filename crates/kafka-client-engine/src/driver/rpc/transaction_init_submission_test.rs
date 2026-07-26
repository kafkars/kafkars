//! Transaction-coordinator routing and original-deadline submission options.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKind, Route, TrafficClass};

use super::transaction_init_submission::{transaction_coordinator_route, transaction_init_options};

#[test]
fn route_uses_the_driver_transaction_coordinator_authority() {
    let route = transaction_coordinator_route("invoice-writer")
        .unwrap_or_else(|error| panic!("valid transaction route: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("transaction initialization must use coordinator routing");
    };
    assert_eq!(key.kind(), CoordinatorKind::Transaction);
    assert_eq!(key.as_str(), "invoice-writer");
}

#[test]
fn options_preserve_original_deadline_interactive_lane_and_v5_ceiling() {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(2))
        .unwrap_or_else(|| panic!("test deadline"));
    let options = transaction_init_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(5)));
}
