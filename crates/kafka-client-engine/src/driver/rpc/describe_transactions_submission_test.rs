//! Admin `DescribeTransactions` transaction-coordinator route and v0 policy scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKind, Route, TrafficClass};

use super::describe_transactions_submission::{
    describe_transactions_options, describe_transactions_route,
};

#[test]
fn route_targets_the_requested_transaction_coordinator() {
    let Route::Coordinator { key } = describe_transactions_route("orders-writer")
        .unwrap_or_else(|error| panic!("valid route: {error}"))
    else {
        panic!("transaction-coordinator route expected");
    };
    assert_eq!(key.kind(), CoordinatorKind::Transaction);
    assert_eq!(key.as_str(), "orders-writer");
    assert!(describe_transactions_route("").is_err());
}

#[test]
fn options_preserve_original_deadline_interactive_lane_and_exact_v0() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = describe_transactions_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}
