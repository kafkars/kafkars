//! API-key 69 coordinator correlation, deadline, lane, and version-window tests.

use std::time::Instant;

use kafka_client_core::Deadline;
use kafka_driver::{ApiVersion, TrafficClass};
use kafka_wire::ConsumerGroupDescribeRequest;

use crate::clock::OperationDeadline;

use super::consumer_group_describe_submission::{
    ConsumerGroupDescribeSubmitError, consumer_group_describe_options,
    consumer_group_describe_route,
};

#[test]
fn options_preserve_original_deadline_interactive_lane_and_v0_v1_bounds() {
    let instant = Instant::now();
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(11), instant);
    let options = consumer_group_describe_options(deadline);
    assert_eq!(options.deadline(), instant);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version().map(ApiVersion::value), Some(0));
    assert_eq!(options.maximum_version().map(ApiVersion::value), Some(1));
}

#[test]
fn route_requires_one_exact_coordinator_correlated_group() {
    let mut request = ConsumerGroupDescribeRequest::default();
    assert!(matches!(
        consumer_group_describe_route("workers", &request),
        Err(ConsumerGroupDescribeSubmitError::InvalidGroupBatch)
    ));
    request.group_ids = vec!["other".into()];
    assert!(matches!(
        consumer_group_describe_route("workers", &request),
        Err(ConsumerGroupDescribeSubmitError::GroupMismatch)
    ));
    request.group_ids[0] = "workers".into();
    assert!(consumer_group_describe_route("workers", &request).is_ok());
    request.group_ids.push("second".into());
    assert!(matches!(
        consumer_group_describe_route("workers", &request),
        Err(ConsumerGroupDescribeSubmitError::InvalidGroupBatch)
    ));
}
