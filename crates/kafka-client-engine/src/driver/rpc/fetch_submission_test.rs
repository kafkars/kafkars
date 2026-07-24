//! Partition-routed long-poll `Fetch` submission scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, RoutedCall, TrafficClass};
use kafka_wire::{FetchRequest, FetchResponse};

use crate::{EngineConfig, protocol::fetch::FETCH_NAME_ROUTE_MAX_VERSION};

use super::{
    super::DriverOwner,
    fetch_submission::{FetchSubmitError, fetch_options},
};

#[test]
fn options_preserve_original_deadline_long_poll_lane_and_name_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = fetch_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::LongPoll);
    assert_eq!(
        options.maximum_version(),
        Some(ApiVersion::new(FETCH_NAME_ROUTE_MAX_VERSION))
    );
}

#[test]
fn accepted_submission_returns_a_tracked_partition_call() {
    let mut owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let call = owner
        .submit_tracked_fetch("events", 3, FetchRequest::default(), deadline)
        .unwrap_or_else(|error| panic!("tracked Fetch admission: {error}"));

    assert!(call.try_result().is_none());
    assert_tracked_fetch(&call);
    drop(call);
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn invalid_route_facts_are_rejected_before_driver_ownership() {
    let owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let empty_topic = owner
        .submit_tracked_fetch("", 0, FetchRequest::default(), deadline)
        .err()
        .unwrap_or_else(|| panic!("empty topic must be rejected"));
    assert!(matches!(empty_topic, FetchSubmitError::InvalidTopic(_)));

    let negative_partition = owner
        .submit_tracked_fetch("events", -1, FetchRequest::default(), deadline)
        .err()
        .unwrap_or_else(|| panic!("negative partition must be rejected"));
    assert!(matches!(
        negative_partition,
        FetchSubmitError::InvalidPartition(_)
    ));
}

fn assert_tracked_fetch(_call: &RoutedCall<FetchResponse>) {}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
