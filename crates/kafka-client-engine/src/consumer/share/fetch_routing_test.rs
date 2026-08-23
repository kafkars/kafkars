//! Assignment-wide share-fetch routing and recovery scenarios.

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

fn broker_id_one() -> ShareFetchBrokerId {
    ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("broker"))
}
