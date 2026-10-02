//! Physical close removal, retained-byte reclamation, and external delivery drain evidence.

use std::sync::Arc;

use kafka_client_core::Moment;

use crate::{EngineConfig, clock::MonotonicClock, driver::DriverOwner};

use super::{
    classic_group_graceful_revocation::ClassicGroupRevocationTurn,
    classic_group_leave::{GroupConsumerCloseCompletionObservation, GroupConsumerCloseTerminal},
    registry::GroupConsumerRegistry,
    registry_entry::default_classic_processing_lease_policy,
    registry_fetch::GroupConsumerFetchTurn,
    registry_membership::{GroupConsumerMembershipTurn, GroupConsumerMembershipTurn::Progress},
    registry_test_support::{
        deadline, install_ready_group_delivery, install_session, register, started_registry,
        stop_registry,
    },
};

#[test]
fn accepted_close_publishes_success_only_after_physical_removal() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let authority = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered group"))
        .close_authority();

    let completion = registry
        .close_group_explicit(group_id, deadline(100), &authority)
        .unwrap_or_else(|error| panic!("explicit close admission: {error:?}"));
    assert_eq!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Pending
    );
    drive_local_close(&mut registry);
    assert_eq!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Pending
    );
    assert_eq!(registry.remove_one_closed_group(), Ok(true));
    assert_eq!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Terminal(GroupConsumerCloseTerminal::Succeeded)
    );
    stop_registry(&mut registry);
}

#[test]
fn static_identity_bytes_are_reclaimed_only_after_physical_removal() {
    let mut registry = started_registry();
    let group_id = registry
        .try_register_with_configuration(
            Arc::from("workers"),
            Some(Arc::from("instance-a")),
            vec![Arc::from("orders")],
            super::classic_group_test_support::timing(),
            super::classic_group_test_support::heartbeat_policy(),
            super::classic_group_test_support::rejoin_policy(),
            kafka_client_core::GroupPositionMissingOffsetPolicy::Error,
            kafka_client_core::ReadIsolation::ReadUncommitted,
            default_classic_processing_lease_policy(),
        )
        .unwrap_or_else(|failure| panic!("static registration: {:?}", failure.kind));
    let retained = registry.retained_group_bytes();

    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));
    assert_eq!(registry.retained_group_bytes(), retained);
    assert_eq!(
        registry.turn_local_membership(Moment::from_tick(u64::MAX)),
        Ok(Progress)
    );
    assert_eq!(registry.remove_one_closed_group(), Ok(true));

    assert_eq!(registry.retained_group_bytes(), 0);
    assert!(registry.entry(group_id).is_none());
    stop_registry(&mut registry);
}

#[test]
fn close_retires_cached_broker_sessions_after_the_assignment_has_already_been_lost() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    entry.fetch.install_retained_initial_session_for_test();
    assert!(entry.fetch.activation().is_none());
    assert!(
        !entry.fetch.is_idle(),
        "the previous broker session is still owned"
    );
    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));
    drive_local_close(&mut registry);
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    drive_fetch_locally(&mut registry, &MonotonicClock::new(), &driver);
    let removed = registry.remove_one_closed_group();
    drop(driver);
    stop_registry(&mut registry);
    assert_eq!(
        removed,
        Ok(true),
        "close must drain sessions retained for an abandoned rejoin"
    );
}

#[test]
fn external_batch_lease_blocks_removal_until_exact_reclaim() {
    let mut registry = started_registry();
    let clock = MonotonicClock::new();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let mut previous_group = None;
    for _cycle in 0..128 {
        let group_id = close_with_retained_delivery(&mut registry, &clock, &driver);
        assert!(previous_group.is_none_or(|previous| group_id > previous));
        previous_group = Some(group_id);
        assert_eq!(registry.registered_group_count(), 0);
        assert_eq!(registry.retained_group_bytes(), 0);
    }
    drop(driver);
    stop_registry(&mut registry);
}

#[test]
fn retained_delivery_does_not_hide_an_elapsed_explicit_close_terminal() {
    retained_delivery_at_close_deadline(false);
}

#[test]
fn successful_broker_leave_retains_the_public_deadline_while_delivery_blocks_cleanup() {
    retained_delivery_at_close_deadline(true);
}

fn retained_delivery_at_close_deadline(broker_leave_succeeded: bool) {
    let mut registry = started_registry();
    let clock = MonotonicClock::new();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    install_ready_group_delivery(&mut registry, group_id, 17);
    let delivery = registry
        .take_delivery(group_id, &clock)
        .unwrap_or_else(|error| panic!("delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready delivery"));
    let authority = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("group"))
        .close_authority();
    let completion = registry
        .close_group_explicit(group_id, deadline(100), &authority)
        .unwrap_or_else(|error| panic!("close: {error:?}"));
    if broker_leave_succeeded {
        let entry = registry
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .unwrap_or_else(|| panic!("group"));
        assert!(
            entry
                .leave
                .resolve_consumer_group(GroupConsumerCloseTerminal::Succeeded)
        );
        assert_eq!(
            entry.leave.next_deadline(),
            Some(kafka_client_core::Deadline::from_tick(100))
        );
        registry
            .turn(Moment::from_tick(99), &clock, &driver)
            .unwrap_or_else(|error| panic!("pre-deadline drain: {error}"));
        assert_eq!(
            completion.observe(),
            GroupConsumerCloseCompletionObservation::Pending
        );
    } else {
        registry.turn_one_classic_group_leave(Moment::from_tick(100), &driver);
    }
    assert!(
        registry
            .turn(Moment::from_tick(100), &clock, &driver)
            .unwrap_or_else(|error| panic!("failure publication: {error}"))
            .progressed
    );
    drain_revocation(&mut registry);
    drive_local_close(&mut registry);
    drive_fetch_locally(&mut registry, &clock, &driver);
    assert_eq!(registry.remove_one_closed_group(), Ok(false));
    let observed_while_retained = completion.observe();
    registry
        .reclaim_delivery(delivery)
        .unwrap_or_else(|_| panic!("reclaim exact delivery"));
    drain_revocation(&mut registry);
    drive_local_close(&mut registry);
    drive_fetch_locally(&mut registry, &clock, &driver);
    assert_eq!(registry.remove_one_closed_group(), Ok(true));
    let final_terminal = completion.observe();
    drop(driver);
    stop_registry(&mut registry);
    assert!(
        matches!(final_terminal, GroupConsumerCloseCompletionObservation::Terminal(
        GroupConsumerCloseTerminal::Failed(failure))
        if failure.kind == super::classic_group_leave::GroupConsumerCloseTerminalFailureKind::DeadlineElapsed)
    );
    assert_eq!(
        observed_while_retained, final_terminal,
        "an expired close must not wait for application-owned batch reclamation"
    );
}

fn close_with_retained_delivery(
    registry: &mut GroupConsumerRegistry,
    clock: &MonotonicClock,
    driver: &DriverOwner,
) -> kafka_client_core::GroupId {
    let group_id = register(registry, "workers");
    install_session(registry, group_id);
    install_ready_group_delivery(registry, group_id, 17);
    let delivery = registry
        .take_delivery(group_id, clock)
        .unwrap_or_else(|error| panic!("delivery observation: {error:?}"))
        .unwrap_or_else(|| panic!("ready group delivery"));
    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));

    drain_revocation(registry);
    drive_local_close(registry);
    drive_fetch_locally(registry, clock, driver);
    assert_eq!(registry.remove_one_closed_group(), Ok(false));
    assert!(
        registry
            .entry(group_id)
            .is_some_and(|entry| entry.fault.is_none()),
        "close must not fault while the exact external lease is retained"
    );

    registry
        .reclaim_delivery(delivery)
        .unwrap_or_else(|_error| panic!("exact delivery reclaim"));
    drain_revocation(registry);
    drive_local_close(registry);
    drive_fetch_locally(registry, clock, driver);
    assert_eq!(registry.remove_one_closed_group(), Ok(true));
    assert!(registry.entry(group_id).is_none());
    group_id
}

fn drain_revocation(registry: &mut GroupConsumerRegistry) {
    for _turn in 0..16 {
        match registry
            .turn_graceful_revocation(Moment::from_tick(u64::MAX))
            .unwrap_or_else(|error| panic!("revocation close turn: {error:?}"))
        {
            ClassicGroupRevocationTurn::Progress => {}
            ClassicGroupRevocationTurn::Idle => break,
        }
    }
}

fn drive_local_close(registry: &mut GroupConsumerRegistry) {
    for _turn in 0..16 {
        match registry
            .turn_local_membership(Moment::from_tick(u64::MAX))
            .unwrap_or_else(|error| panic!("membership close turn: {error:?}"))
        {
            GroupConsumerMembershipTurn::Progress => {}
            GroupConsumerMembershipTurn::Idle | GroupConsumerMembershipTurn::Blocked => break,
        }
    }
}

fn drive_fetch_locally(
    registry: &mut GroupConsumerRegistry,
    clock: &MonotonicClock,
    driver: &DriverOwner,
) {
    for _turn in 0..16 {
        match registry
            .turn_fetch(clock, driver)
            .unwrap_or_else(|error| panic!("Fetch close turn: {error:?}"))
        {
            GroupConsumerFetchTurn::Progress => {}
            GroupConsumerFetchTurn::Idle | GroupConsumerFetchTurn::Blocked => break,
        }
    }
}
