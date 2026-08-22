//! Modern dormant-release fencing and assignmentless shutdown accounting.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    ConsumerGroupHeartbeatPhase, GroupId, GroupPositionMissingOffsetPolicy, Moment, ReadIsolation,
};

use crate::{
    clock::MonotonicClock,
    config::{ValidatedConsumerFetchConfig, ValidatedConsumerLimits},
    consumer::group_registration_request::{GroupConsumerClassicAssignor, GroupConsumerProtocol},
};

use super::{
    super::{
        classic_group_test_support,
        consumer_group_close::ConsumerGroupCloseTurn,
        consumer_group_heartbeat_settlement::{
            ConsumerGroupHeartbeatSettlementTurn, settle_success,
        },
        consumer_group_heartbeat_settlement_test::success_without_assignment,
        registry::GroupConsumerRegistry,
        registry_entry::default_classic_processing_lease_policy,
        registry_host_error::GroupConsumerHostError,
    },
    GroupConsumerDormantUnregisterError,
};

#[test]
fn pristine_modern_registration_can_be_released() {
    let mut registry = started_registry();
    let group_id = register_modern(&mut registry, "pristine");

    assert_eq!(registry.try_unregister_dormant(group_id), Ok(()));
    assert!(registry.entry(group_id).is_none());
    stop_registry(&mut registry);
}

#[test]
fn begun_modern_join_cannot_be_released_as_dormant() {
    let mut registry = started_registry();
    let group_id = register_modern(&mut registry, "joining");
    begin_modern(&mut registry, group_id);

    assert_eq!(
        registry.try_unregister_dormant(group_id),
        Err(GroupConsumerDormantUnregisterError::NotDormant)
    );
    assert!(registry.entry(group_id).is_some());
    stop_registry(&mut registry);
}

#[test]
fn assignmentless_accepted_member_cannot_be_released_as_dormant() {
    let mut registry = started_registry();
    let group_id = register_modern(&mut registry, "awaiting");
    await_assignment(&mut registry, group_id);

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("retained modern member"));
    assert!(entry.catalog.current_member_id().is_some());
    assert!(entry.catalog.live_assignment().is_none());
    assert_eq!(
        registry.try_unregister_dormant(group_id),
        Err(GroupConsumerDormantUnregisterError::NotDormant)
    );
    assert!(registry.entry(group_id).is_some());
    stop_registry(&mut registry);
}

#[test]
fn closing_assignmentless_member_counts_until_locally_closed() {
    let mut registry = started_registry();
    let group_id = register_modern(&mut registry, "closing");
    await_assignment(&mut registry, group_id);
    registry.close_admission();

    assert_eq!(registry.membership_unsettled(), 1);
    assert_eq!(
        registry.turn_one_consumer_group_close(Moment::from_tick(u64::MAX)),
        Ok(ConsumerGroupCloseTurn::Progress)
    );
    assert_eq!(registry.membership_unsettled(), 0);
    stop_registry(&mut registry);
}

#[test]
fn shutdown_waits_for_each_assignmentless_modern_member() {
    let mut registry = started_registry();
    let first = register_modern(&mut registry, "first");
    let second = register_modern(&mut registry, "second");
    await_assignment(&mut registry, first);
    await_assignment(&mut registry, second);
    registry.close_admission();

    assert_eq!(registry.membership_unsettled(), 2);
    let first_error = registry
        .finish_shutdown()
        .err()
        .unwrap_or_else(|| panic!("two pending members must block shutdown"));
    assert_eq!(first_error, GroupConsumerHostError::membership_unsettled(2));
    assert_eq!(
        registry.turn_one_consumer_group_close(Moment::from_tick(u64::MAX)),
        Ok(ConsumerGroupCloseTurn::Progress)
    );
    let second_error = registry
        .finish_shutdown()
        .err()
        .unwrap_or_else(|| panic!("one pending member must block shutdown"));
    assert_eq!(
        second_error,
        GroupConsumerHostError::membership_unsettled(1)
    );
    assert_eq!(
        registry.turn_one_consumer_group_close(Moment::from_tick(u64::MAX)),
        Ok(ConsumerGroupCloseTurn::Progress)
    );
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finished modern shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

fn register_modern(registry: &mut GroupConsumerRegistry, group: &str) -> GroupId {
    registry
        .try_register_with_protocol_configuration(
            Arc::from(group),
            None,
            vec![Arc::from("orders")],
            GroupConsumerProtocol::Consumer,
            GroupConsumerClassicAssignor::Range,
            classic_group_test_support::timing(),
            classic_group_test_support::heartbeat_policy(),
            classic_group_test_support::rejoin_policy(),
            GroupPositionMissingOffsetPolicy::Error,
            ReadIsolation::ReadUncommitted,
            default_classic_processing_lease_policy(),
            ValidatedConsumerFetchConfig::default(),
            ValidatedConsumerLimits::default(),
        )
        .unwrap_or_else(|failure| panic!("modern registration: {:?}", failure.kind))
}

fn begin_modern(registry: &mut GroupConsumerRegistry, group_id: GroupId) -> Moment {
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("modern deadline: {error}"));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered modern entry"));
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin modern membership: {error:?}"));
    capture.now()
}

fn await_assignment(registry: &mut GroupConsumerRegistry, group_id: GroupId) {
    let now = begin_modern(registry, group_id);
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered modern entry"));
    assert_eq!(
        settle_success(entry, now, success_without_assignment(1)),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    assert_eq!(
        entry
            .consumer
            .as_ref()
            .map(|consumer| consumer.machine().phase()),
        Some(ConsumerGroupHeartbeatPhase::AwaitingAssignment)
    );
}

fn started_registry() -> GroupConsumerRegistry {
    GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry start: {error}"))
}

fn stop_registry(registry: &mut GroupConsumerRegistry) {
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
