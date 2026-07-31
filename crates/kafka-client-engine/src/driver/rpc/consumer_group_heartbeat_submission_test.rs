//! KIP-848 heartbeat route, deadline, traffic, and version-window evidence.

use std::time::{Duration, Instant};

use kafka_client_core::Deadline;
use kafka_driver::TrafficClass;
use kafka_wire::{CONSUMER_GROUP_HEARTBEAT_API_DESCRIPTOR, ConsumerGroupHeartbeatRequest};

use crate::clock::OperationDeadline;

use super::consumer_group_heartbeat_submission::{
    CONSUMER_HEARTBEAT_MAX_VERSION, CONSUMER_HEARTBEAT_MIN_VERSION,
    ConsumerGroupHeartbeatSubmitError, consumer_group_heartbeat_options,
    consumer_group_heartbeat_route,
};

#[test]
fn options_preserve_absolute_deadline_control_lane_and_exact_v0_window() {
    let transport = Instant::now() + Duration::from_secs(5);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(50), transport);
    let options = consumer_group_heartbeat_options(deadline);
    assert_eq!(options.deadline(), transport);
    assert_eq!(options.traffic_class(), TrafficClass::Control);
    assert_eq!(
        options.minimum_version(),
        Some(CONSUMER_HEARTBEAT_MIN_VERSION)
    );
    assert_eq!(
        options.maximum_version(),
        Some(CONSUMER_HEARTBEAT_MAX_VERSION)
    );
}

#[test]
fn first_beta_window_is_an_exact_wire_supported_subset() {
    let supported = CONSUMER_GROUP_HEARTBEAT_API_DESCRIPTOR.supported_versions;
    assert!(supported.contains(CONSUMER_HEARTBEAT_MIN_VERSION));
    assert!(supported.contains(CONSUMER_HEARTBEAT_MAX_VERSION));
    assert_eq!(CONSUMER_HEARTBEAT_MIN_VERSION.value(), 0);
    assert_eq!(CONSUMER_HEARTBEAT_MAX_VERSION.value(), 0);
}

#[test]
fn coordinator_and_generated_request_group_spellings_must_match() {
    let mut request = ConsumerGroupHeartbeatRequest::default();
    request.group_id = "group-b".into();
    assert!(matches!(
        consumer_group_heartbeat_route("group-a", &request),
        Err(ConsumerGroupHeartbeatSubmitError::GroupMismatch)
    ));
    assert!(consumer_group_heartbeat_route("group-b", &request).is_ok());
}
