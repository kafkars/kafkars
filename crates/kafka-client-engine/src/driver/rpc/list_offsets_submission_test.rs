//! Partition-routed `ListOffsets` submission scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, RoutedCall, TrafficClass};
use kafka_wire::{ListOffsetsRequest, ListOffsetsResponse};

use crate::EngineConfig;

use super::{
    super::DriverOwner,
    list_offsets_submission::{ListOffsetsSubmitError, list_offsets_options},
};

#[test]
fn options_preserve_original_deadline_interactive_lane_and_v11_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = list_offsets_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(11)));
}

#[test]
fn accepted_submission_returns_a_tracked_partition_call() {
    let mut owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let call = owner
        .submit_tracked_list_offsets("orders", 3, ListOffsetsRequest::default(), deadline)
        .unwrap_or_else(|error| panic!("tracked ListOffsets admission: {error}"));

    assert!(call.try_result().is_none());
    assert_tracked_list_offsets(&call);
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
        .submit_tracked_list_offsets("", 0, ListOffsetsRequest::default(), deadline)
        .err()
        .unwrap_or_else(|| panic!("empty topic must be rejected"));
    assert!(matches!(
        empty_topic,
        ListOffsetsSubmitError::InvalidTopic(_)
    ));

    let negative_partition = owner
        .submit_tracked_list_offsets("orders", -1, ListOffsetsRequest::default(), deadline)
        .err()
        .unwrap_or_else(|| panic!("negative partition must be rejected"));
    assert!(matches!(
        negative_partition,
        ListOffsetsSubmitError::InvalidPartition(_)
    ));
}

fn assert_tracked_list_offsets(_call: &RoutedCall<ListOffsetsResponse>) {}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
