//! Transaction-offset coordinator routes and bounded v3-v4 submission options.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKeyError, CoordinatorKind, Route, TrafficClass};

use super::submission::{add_offsets_route, offset_commit_route, transaction_offset_options};

#[test]
fn each_request_uses_its_protocol_coordinator_authority() {
    let add_offsets =
        add_offsets_route("invoice-writer").unwrap_or_else(|error| panic!("route: {error}"));
    let Route::Coordinator { key } = add_offsets else {
        panic!("AddOffsetsToTxn must use coordinator routing");
    };
    assert_eq!(key.kind(), CoordinatorKind::Transaction);
    assert_eq!(key.as_str(), "invoice-writer");

    let offset_commit =
        offset_commit_route("invoice-workers").unwrap_or_else(|error| panic!("route: {error}"));
    let Route::Coordinator { key } = offset_commit else {
        panic!("TxnOffsetCommit must use coordinator routing");
    };
    assert_eq!(key.kind(), CoordinatorKind::Group);
    assert_eq!(key.as_str(), "invoice-workers");
}

#[test]
fn empty_coordinator_keys_are_rejected_before_driver_submission() {
    assert!(matches!(
        add_offsets_route(""),
        Err(CoordinatorKeyError::Empty)
    ));
    assert!(matches!(
        offset_commit_route(""),
        Err(CoordinatorKeyError::Empty)
    ));
}

#[test]
fn options_preserve_deadline_lane_and_v3_v4_bounds() {
    let deadline = Instant::now() + Duration::from_secs(2);
    let options = transaction_offset_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(3)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(4)));
}
