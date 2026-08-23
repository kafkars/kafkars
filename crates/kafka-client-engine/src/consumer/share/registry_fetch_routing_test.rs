//! Hosted share-fetch routing, assignment fencing, and close scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::Moment;

use crate::{
    EngineConfig,
    clock::MonotonicClock,
    driver::{DriverOwner, RoutedBroker, TopicPartitionCountFact},
    protocol::consumer::share_group::share_group_heartbeat_success_for_test,
};

use super::{
    registry::ShareConsumerRegistry, registry_fetch_routing::ShareFetchRoutingHostTurn,
    registry_topic_identity::complete_topic_identity,
};

#[test]
fn hosted_assignment_becomes_one_retained_broker_plan() {
    let (mut registry, group_id, clock, capture) = registry_with_routable_membership();
    settle_partition_three(&mut registry, group_id, capture.now());
    let (mut broker, mut driver) = routed_driver();

    assert_eq!(
        registry
            .turn_one_fetch_routing(capture.now(), &clock, &driver)
            .unwrap_or_else(|error| panic!("start routing: {error:?}")),
        ShareFetchRoutingHostTurn::Progress
    );
    assert_eq!(
        registry
            .turn_one_fetch_routing(capture.now(), &clock, &driver)
            .unwrap_or_else(|error| panic!("submit routing: {error:?}")),
        ShareFetchRoutingHostTurn::Progress
    );
    broker.install_topic(&mut driver);
    for _turn in 0..32 {
        let _turn = registry
            .turn_one_fetch_routing(capture.now(), &clock, &driver)
            .unwrap_or_else(|error| panic!("drive routing: {error:?}"));
        if registry
            .entry(group_id)
            .is_some_and(|entry| entry.fetch().routed().is_some())
        {
            break;
        }
        drive(&mut driver);
    }
    let entry = registry.entry(group_id).unwrap_or_else(|| panic!("entry"));
    let routed = entry.fetch().routed().unwrap_or_else(|| {
        panic!(
            "routed broker assignment: routing={} fault={:?}",
            entry.fetch().routing().is_some(),
            entry
                .fetch()
                .fault()
                .map(super::fetch_state::ShareFetchRoutingFault::kind)
        )
    });
    assert_eq!(routed.generation().get(), 1);
    assert_eq!(registry.unsettled(), 4);
    assert!(registry.next_deadline().is_some());

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover: {error:?}"));
}

#[test]
fn close_abandons_routing_before_any_driver_call_is_admitted() {
    let (mut registry, group_id, clock, capture) = registry_with_routable_membership();
    settle_partition_three(&mut registry, group_id, capture.now());
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    assert_eq!(
        registry
            .turn_one_fetch_routing(capture.now(), &clock, &driver)
            .unwrap_or_else(|error| panic!("start routing: {error:?}")),
        ShareFetchRoutingHostTurn::Progress
    );
    registry.request_control_close(capture);
    assert_eq!(
        registry
            .turn_one_fetch_routing(capture.now(), &clock, &driver)
            .unwrap_or_else(|error| panic!("abandon routing: {error:?}")),
        ShareFetchRoutingHostTurn::Progress
    );
    assert!(
        registry
            .entry(group_id)
            .is_some_and(|entry| entry.fetch().routing().is_none())
    );

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover: {error:?}"));
}

fn settle_partition_three(
    registry: &mut super::registry::ShareConsumerRegistry,
    group_id: kafka_client_core::GroupId,
    now: Moment,
) {
    let entry = registry
        .entry_mut(group_id)
        .unwrap_or_else(|| panic!("entry"));
    let member = std::sync::Arc::clone(entry.member());
    entry
        .membership
        .as_mut()
        .unwrap_or_else(|| panic!("membership"))
        .settle_success(
            now,
            share_group_heartbeat_success_for_test(
                Some(&member),
                1,
                5_000,
                vec![([7; 16], vec![3])],
            ),
        )
        .unwrap_or_else(|error| panic!("assignment: {error:?}"));
}

fn registry_with_routable_membership() -> (
    ShareConsumerRegistry,
    kafka_client_core::GroupId,
    MonotonicClock,
    crate::clock::DeadlineCapture,
) {
    let clock = MonotonicClock::new();
    let mut registry =
        ShareConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let capture = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let group_id = registry
        .try_register(Arc::from("workers"), None, vec![Arc::from("events")])
        .unwrap_or_else(|_error| panic!("register"));
    registry
        .try_begin(group_id, capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    let entry = registry
        .entry_mut(group_id)
        .unwrap_or_else(|| panic!("entry"));
    let local_topic_id = entry
        .local_topic_id(0)
        .unwrap_or_else(|| panic!("topic id"));
    complete_topic_identity(
        entry,
        local_topic_id,
        Arc::from("events"),
        capture.operation_deadline(),
        TopicPartitionCountFact {
            metadata_generation: 1,
            logical_partition_count: 4,
            kafka_topic_id: Some([7; 16]),
        },
    )
    .unwrap_or_else(|error| panic!("topic identity: {error:?}"));
    (registry, group_id, clock, capture)
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
