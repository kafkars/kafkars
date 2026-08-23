//! Driver-authoritative `ShareFetch` partition-routing scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    AssignmentGeneration, GroupAssignmentPartition, GroupId, LiveGroupAssignment, MemberId,
    PartitionIndex, ShareFetchBrokerId, TopicId,
};

use crate::{
    EngineConfig,
    clock::MonotonicClock,
    driver::{DriverOwner, RoutedBroker},
};

use super::{
    catalog::{ShareMembershipCatalog, ShareTopicIdentity},
    fetch_route::{
        ShareFetchPartitionRouteCall, ShareFetchPartitionRouteFailureKind,
        ShareFetchPartitionRouteRequest,
    },
};

#[test]
fn current_topic_uuid_and_partition_bind_the_driver_observed_broker() {
    let (mut broker, mut driver) = routed_driver();
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(60))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let request = request(&catalog([7; 16]), &assignment(), capture);
    let mut call = ShareFetchPartitionRouteCall::submit(&driver, request, capture.now())
        .unwrap_or_else(|failure| panic!("route admission: {:?}", failure.kind()));
    broker.install_topic(&mut driver);

    let routed = (0..32)
        .find_map(|_| {
            call.try_terminal().or_else(|| {
                drive(&mut driver);
                call.try_terminal()
            })
        })
        .unwrap_or_else(|| panic!("share route did not settle"))
        .unwrap_or_else(|failure| panic!("share route: {:?}", failure.kind()));
    assert_eq!(
        routed.broker_id(),
        ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("broker"))
    );
    let request = routed.into_request();
    assert_eq!(request.partition(), assignment().partitions()[0]);
    assert_eq!(request.assignment_generation().get(), 4);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
}

#[test]
fn changed_topic_identity_returns_the_exact_assignment_request() {
    let (mut broker, mut driver) = routed_driver();
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(60))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let request = request(&catalog([8; 16]), &assignment(), capture);
    let expected = request.partition();
    let mut call = ShareFetchPartitionRouteCall::submit(&driver, request, capture.now())
        .unwrap_or_else(|failure| panic!("route admission: {:?}", failure.kind()));
    broker.install_topic(&mut driver);

    let failure = (0..32)
        .find_map(|_| {
            call.try_terminal().or_else(|| {
                drive(&mut driver);
                call.try_terminal()
            })
        })
        .unwrap_or_else(|| panic!("share route did not settle"))
        .err()
        .unwrap_or_else(|| panic!("changed topic identity must fail"));
    assert_eq!(
        failure.kind(),
        ShareFetchPartitionRouteFailureKind::TopicIdentityChanged
    );
    assert_eq!(failure.into_request().partition(), expected);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
}

#[test]
fn driver_shutdown_recovers_the_unsettled_assignment_request() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(60))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let request = request(&catalog([7; 16]), &assignment(), capture);
    let expected = request.partition();
    let call = ShareFetchPartitionRouteCall::submit(&driver, request, capture.now())
        .unwrap_or_else(|failure| panic!("route admission: {:?}", failure.kind()));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
    assert_eq!(call.recover_after_driver_shutdown().partition(), expected);
}

fn routed_driver() -> (RoutedBroker, DriverOwner) {
    let mut broker = RoutedBroker::new();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec![broker.endpoint()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    RoutedBroker::await_seed(&mut driver);
    broker.install_cluster(&mut driver);
    (broker, driver)
}

fn drive(driver: &mut DriverOwner) {
    driver
        .turn(Duration::from_millis(100))
        .unwrap_or_else(|error| panic!("drive: {error}"));
}

fn request(
    catalog: &ShareMembershipCatalog,
    assignment: &LiveGroupAssignment,
    capture: crate::clock::DeadlineCapture,
) -> ShareFetchPartitionRouteRequest {
    ShareFetchPartitionRouteRequest::try_at(catalog, assignment, 0, capture)
        .unwrap_or_else(|error| panic!("request: {error:?}"))
}

fn catalog(kafka_topic_id: [u8; 16]) -> ShareMembershipCatalog {
    ShareMembershipCatalog::try_new(
        Arc::from("share"),
        Arc::from("member"),
        None,
        vec![ShareTopicIdentity::new(
            TopicId::from_raw(1),
            Arc::from("events"),
            kafka_topic_id,
            4,
        )],
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"))
}

fn assignment() -> LiveGroupAssignment {
    LiveGroupAssignment::try_new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(4).unwrap_or_else(|| panic!("generation")),
        vec![GroupAssignmentPartition::new(
            TopicId::from_raw(1),
            PartitionIndex::from_raw(3),
        )],
    )
    .unwrap_or_else(|error| panic!("assignment: {error:?}"))
}
