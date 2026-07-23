//! Tracked name-routed Produce submission scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiVersion, RoutedCall, TrafficClass};
use kafka_wire::{ProduceRequest, ProduceResponse};

use crate::EngineConfig;

use super::{
    DriverOwner,
    rpc::{ProduceSubmitError, produce_options},
};

#[test]
fn produce_options_preserve_deadline_lane_and_name_route_version_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = produce_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Bulk);
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(12)));
}

#[test]
fn accepted_submission_returns_the_token_retaining_tracked_call() {
    let mut owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let call = owner
        .submit_tracked_produce("orders", 3, ProduceRequest::default(), deadline)
        .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"));

    assert!(call.try_result().is_none());
    assert_tracked_produce(&call);
    drop(call);
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn invalid_route_and_closed_driver_rejections_are_not_sent() {
    let mut owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let invalid = owner
        .submit_tracked_produce("", 0, ProduceRequest::default(), deadline)
        .err()
        .unwrap_or_else(|| panic!("empty topic must be rejected"));
    assert_eq!(invalid.delivery(), DeliveryStatus::NotSent);

    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
    let closed = owner
        .submit_tracked_produce("orders", 0, ProduceRequest::default(), deadline)
        .err()
        .unwrap_or_else(|| panic!("closed driver must reject admission"));
    assert!(matches!(closed, ProduceSubmitError::Driver(_)));
    assert_eq!(closed.delivery(), DeliveryStatus::NotSent);
}

fn assert_tracked_produce(_call: &RoutedCall<ProduceResponse>) {}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
