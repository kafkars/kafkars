//! Assignment-wide share-fetch routing and recovery scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    AssignmentGeneration, GroupAssignmentPartition, GroupId, LiveGroupAssignment, MemberId, Moment,
    PartitionIndex, ShareFetchBrokerId, TopicId,
};

use crate::{
    EngineConfig,
    clock::MonotonicClock,
    driver::{DriverOwner, RoutedBroker, TopicPartitionCountFailure},
};

use super::{
    catalog::{ShareMembershipCatalog, ShareTopicIdentity},
    fetch_route::{
        ShareFetchPartitionRouteFailure, ShareFetchPartitionRouteFailureKind,
        ShareFetchPartitionRouteRequest,
    },
    fetch_routing::{ShareFetchRoutingOwner, ShareFetchRoutingTurn},
};

#[test]
fn complete_assignment_routes_into_one_broker_plan_under_the_original_boundary() {
    let (mut broker, mut driver) = routed_driver();
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(60))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let catalog = catalog();
    let assignment = assignment();
    let mut owner = ShareFetchRoutingOwner::try_begin(&catalog, &assignment, capture)
        .unwrap_or_else(|error| panic!("begin routing: {error:?}"));

    assert_eq!(
        owner.turn(&driver, capture.now()),
        ShareFetchRoutingTurn::Progress
    );
    broker.install_topic(&mut driver);
    for _turn in 0..32 {
        if owner.turn(&driver, capture.now()) == ShareFetchRoutingTurn::Complete {
            break;
        }
        drive(&mut driver);
    }
    let routed = owner
        .try_take_routed_assignment(&catalog)
        .unwrap_or_else(|error| panic!("complete routing: {error:?}"));
    assert_eq!(routed.generation().get(), 4);
    assert_eq!(routed.capture(), capture);
    let mut plans = routed.into_plans();
    assert_eq!(plans.len(), 1);
    let (broker_id, partitions, _request) = plans
        .pop()
        .unwrap_or_else(|| panic!("one broker plan"))
        .into_parts();
    assert_eq!(broker_id, broker_id_one());
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].topic_id(), TopicId::from_raw(1));
    assert_eq!(partitions[0].partition(), PartitionIndex::from_raw(3));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
}

#[test]
fn driver_shutdown_restores_the_active_partition_to_pending_routing() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(60))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let catalog = catalog();
    let mut owner = ShareFetchRoutingOwner::try_begin(&catalog, &assignment(), capture)
        .unwrap_or_else(|error| panic!("begin routing: {error:?}"));
    assert_eq!(
        owner.turn(&driver, capture.now()),
        ShareFetchRoutingTurn::Progress
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));

    assert!(owner.recover_after_driver_shutdown());
    assert!(owner.try_take_routed_assignment(&catalog).is_err());
}

#[test]
fn transient_leader_loss_retries_after_positive_delay_under_the_original_deadline() {
    let (mut broker, mut driver) = routed_driver();
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(60))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let catalog = catalog();
    let mut owner = ShareFetchRoutingOwner::try_begin(&catalog, &empty_assignment(), capture)
        .unwrap_or_else(|error| panic!("begin routing: {error:?}"));

    assert_eq!(
        owner.settle_terminal(
            Err(route_failure(
                &catalog,
                capture,
                ShareFetchPartitionRouteFailureKind::LeaderUnavailable,
            )),
            capture.now(),
        ),
        ShareFetchRoutingTurn::Progress
    );
    let retry = owner.next_deadline();
    assert!(retry.tick() > capture.now().tick());
    assert!(retry.tick() < owner.deadline().tick());
    assert_eq!(
        owner.turn(&driver, capture.now()),
        ShareFetchRoutingTurn::Blocked
    );
    assert_eq!(
        owner.turn(&driver, Moment::from_tick(retry.tick())),
        ShareFetchRoutingTurn::Progress
    );

    broker.install_topic(&mut driver);
    for _turn in 0..32 {
        if owner.turn(&driver, Moment::from_tick(retry.tick())) == ShareFetchRoutingTurn::Complete {
            break;
        }
        drive(&mut driver);
    }
    assert!(owner.try_take_routed_assignment(&catalog).is_ok());
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
}

#[test]
fn metadata_unavailable_retries_but_semantic_identity_failure_remains_terminal() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(60))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let catalog = catalog();
    let mut transient = ShareFetchRoutingOwner::try_begin(&catalog, &empty_assignment(), capture)
        .unwrap_or_else(|error| panic!("begin transient routing: {error:?}"));
    assert_eq!(
        transient.settle_terminal(
            Err(route_failure(
                &catalog,
                capture,
                ShareFetchPartitionRouteFailureKind::TopicView(
                    TopicPartitionCountFailure::Unavailable,
                ),
            )),
            capture.now(),
        ),
        ShareFetchRoutingTurn::Progress
    );
    assert!(transient.next_deadline().tick() > capture.now().tick());

    let mut semantic = ShareFetchRoutingOwner::try_begin(&catalog, &empty_assignment(), capture)
        .unwrap_or_else(|error| panic!("begin semantic routing: {error:?}"));
    assert_eq!(
        semantic.settle_terminal(
            Err(route_failure(
                &catalog,
                capture,
                ShareFetchPartitionRouteFailureKind::TopicIdentityChanged,
            )),
            capture.now(),
        ),
        ShareFetchRoutingTurn::Progress
    );
    assert_eq!(
        semantic.turn(&driver, capture.now()),
        ShareFetchRoutingTurn::Faulted(ShareFetchPartitionRouteFailureKind::TopicIdentityChanged)
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
}

#[test]
fn transient_route_failure_at_the_original_deadline_terminalizes_as_deadline() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(60))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let catalog = catalog();
    let mut owner = ShareFetchRoutingOwner::try_begin(&catalog, &empty_assignment(), capture)
        .unwrap_or_else(|error| panic!("begin routing: {error:?}"));
    let elapsed = Moment::from_tick(capture.deadline().tick());
    assert_eq!(
        owner.settle_terminal(
            Err(route_failure(
                &catalog,
                capture,
                ShareFetchPartitionRouteFailureKind::LeaderUnavailable,
            )),
            elapsed,
        ),
        ShareFetchRoutingTurn::Progress
    );
    assert_eq!(
        owner.turn(&driver, elapsed),
        ShareFetchRoutingTurn::Faulted(ShareFetchPartitionRouteFailureKind::Deadline)
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
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

fn catalog() -> ShareMembershipCatalog {
    ShareMembershipCatalog::try_new(
        Arc::from("share"),
        Arc::from("member"),
        None,
        vec![ShareTopicIdentity::new(
            TopicId::from_raw(1),
            Arc::from("events"),
            [7; 16],
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

fn empty_assignment() -> LiveGroupAssignment {
    LiveGroupAssignment::try_new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(4).unwrap_or_else(|| panic!("generation")),
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("empty assignment: {error:?}"))
}

fn route_failure(
    catalog: &ShareMembershipCatalog,
    capture: crate::clock::DeadlineCapture,
    kind: ShareFetchPartitionRouteFailureKind,
) -> ShareFetchPartitionRouteFailure {
    let request = ShareFetchPartitionRouteRequest::try_at(catalog, &assignment(), 0, capture)
        .unwrap_or_else(|error| panic!("route request: {error:?}"));
    ShareFetchPartitionRouteFailure::for_test(request, kind)
}

fn broker_id_one() -> ShareFetchBrokerId {
    ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("broker"))
}
