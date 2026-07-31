//! Fetch broker-route admission and exact request-recovery scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition, AssignedTopicPartition,
    NextFetchOffset, PartitionIndex, StartPosition, TopicId,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings},
};

use super::{
    super::super::DriverOwner,
    admission::PartitionFetchRequest,
    route::{BrokerFetchRouteCall, BrokerFetchRouteFailureKind},
};

#[test]
fn invalid_topic_returns_exact_request_before_driver_ownership() {
    let owner = owner();
    let request = request("");
    let expected = request.fence();
    let failure = BrokerFetchRouteCall::submit(&owner, request)
        .err()
        .unwrap_or_else(|| panic!("empty topic must fail"));
    let (request, kind) = failure.into_parts();
    assert_eq!(request.fence(), expected);
    assert!(matches!(kind, BrokerFetchRouteFailureKind::Terminal(_)));
}

#[test]
fn shutdown_recovery_returns_the_unsettled_exact_request() {
    let mut owner = owner();
    let request = request("events");
    let expected = request.fence();
    let call = BrokerFetchRouteCall::submit(&owner, request)
        .unwrap_or_else(|_failure| panic!("topic-view admission"));
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
    assert_eq!(call.recover_after_driver_shutdown().fence(), expected);
}

fn request(topic: &str) -> PartitionFetchRequest {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(3)),
                StartPosition::Offset(
                    NextFetchOffset::try_from_raw(42)
                        .unwrap_or_else(|| panic!("nonnegative offset")),
                ),
            )],
            now: kafka_client_core::Moment::from_tick(0),
            resolution_deadline: kafka_client_core::Deadline::from_tick(u64::MAX),
        })
        .unwrap_or_else(|error| panic!("assignment: {error}"));
    PartitionFetchRequest::from_effect(
        transition.effects()[0],
        topic.to_owned(),
        FetchRequestSettings::new(500, 1, 1024, 1024, 0),
        FetchDecodeLimits::default(),
        OperationDeadline::from_parts_for_test(
            kafka_client_core::Deadline::from_tick(u64::MAX),
            Instant::now() + Duration::from_secs(60),
        ),
    )
    .unwrap_or_else(|error| panic!("prepared request: {error:?}"))
}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build driver: {error}"))
}
