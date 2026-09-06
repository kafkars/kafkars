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
    route::{BrokerFetchRouteCall, BrokerFetchRouteFailureKind, BrokerId},
    routed_response_broker_test::{RoutedBroker, drive},
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

#[test]
fn topic_view_binds_exact_uuid_leader_epoch_and_broker_before_fetch_admission() {
    let mut broker = RoutedBroker::new();
    let mut owner = DriverOwner::build(&EngineConfig::new(vec![broker.endpoint()]))
        .unwrap_or_else(|error| panic!("build routed Fetch driver: {error}"));
    RoutedBroker::await_seed(&mut owner);
    broker.install_cluster(&mut owner);
    let mut call = BrokerFetchRouteCall::submit(&owner, request("events"))
        .unwrap_or_else(|_failure| panic!("topic-view admission"));
    broker.install_topic(&mut owner);
    let routed = settle_route(&mut call, &mut owner)
        .unwrap_or_else(|failure| panic!("Fetch route: {:?}", failure.into_parts().1));
    let (request, broker_id) = routed.into_parts();

    assert_eq!(
        broker_id,
        BrokerId::new(1).unwrap_or_else(|error| panic!("broker ID: {error}"))
    );
    let route = request
        .topic_route()
        .unwrap_or_else(|| panic!("topic route"));
    assert_eq!(route.topic_id(), [7; 16]);
    assert_eq!(route.leader_epoch(), Some(9));
    assert!(route.metadata_generation().is_some());
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
}

#[allow(
    clippy::result_large_err,
    reason = "the test helper must inspect exact routed and rejected request owners"
)]
fn settle_route(
    call: &mut BrokerFetchRouteCall,
    owner: &mut DriverOwner,
) -> Result<super::route_correlation::BrokerRoutedFetch, super::route::BrokerFetchRouteFailure> {
    (0..32)
        .find_map(|_| {
            call.try_terminal().or_else(|| {
                drive(owner, Duration::from_millis(100), "settle Fetch TopicView");
                call.try_terminal()
            })
        })
        .unwrap_or_else(|| panic!("Fetch TopicView did not settle"))
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
