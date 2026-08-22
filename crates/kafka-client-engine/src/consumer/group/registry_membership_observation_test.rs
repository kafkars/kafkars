//! Membership unsettled-accounting and hidden-deadline scenarios.

use std::sync::Arc;

use kafka_client_core::LiveGroupAssignment;

use crate::consumer::{GroupConsumerMembershipEpoch, GroupConsumerProtocol};

use super::{
    classic_group_rejoin_test_support::{arm_rejoin, entry_mut},
    consumer_group_heartbeat_settlement_test::{
        installed_modern_entry, installed_modern_entry_with_instance,
    },
    registry::GroupConsumerRegistry,
    registry_event::GroupConsumerStateError,
    registry_event_reconciliation_test::{
        defer_rejoin_during_reconciliation, prepared_reconciliation,
    },
    registry_test_support::{register, started_registry, stop_registry},
    session_catalog::CurrentGroupSession,
};

#[test]
fn rediscovery_counts_separately_and_hides_its_rejoin_deadline() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let schedule = arm_rejoin(&mut registry, group_id, 10);
    entry_mut(&mut registry, group_id)
        .rediscovery
        .prepare_rediscovery_install()
        .unwrap_or_else(|error| panic!("rediscovery install failed: {error:?}"))
        .commit();

    assert_eq!(registry.membership_unsettled(), 2);
    assert_eq!(registry.membership_next_deadline(), None);

    entry_mut(&mut registry, group_id)
        .rediscovery
        .clear_rediscovery_after_driver_shutdown();
    assert_eq!(registry.membership_unsettled(), 1);
    assert_eq!(registry.membership_next_deadline(), Some(schedule.due()));
    stop_registry(&mut registry);
}

#[test]
fn prepared_classic_reconciliation_counts_as_one_membership_owner() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    let original = core::mem::replace(entry, prepared_reconciliation());

    assert_eq!(registry.membership_unsettled(), 1);

    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("prepared group"));
    let _prepared = core::mem::replace(entry, original);
    assert_eq!(registry.membership_unsettled(), 0);
    stop_registry(&mut registry);
}

#[test]
fn prepared_classic_reconciliation_hides_its_deferred_rejoin_deadline() {
    let mut pending = prepared_reconciliation();
    let schedule = defer_rejoin_during_reconciliation(&mut pending);

    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let entry = entry_mut(&mut registry, group_id);
    let original = core::mem::replace(entry, pending);

    assert_eq!(
        entry_mut(&mut registry, group_id).rejoin.next_deadline(),
        Some(schedule.due())
    );
    assert_eq!(registry.membership_next_deadline(), None);

    let entry = entry_mut(&mut registry, group_id);
    let _pending = core::mem::replace(entry, original);
    stop_registry(&mut registry);
}

#[test]
fn confirmed_static_modern_state_exposes_identity_without_sending_it_transactionally() {
    let (entry, _topic_id) = installed_modern_entry_with_instance(Some(&Arc::from("instance-a")));
    let group_id = entry.group_id();
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);

    let state = registry
        .group_state(group_id)
        .unwrap_or_else(|error| panic!("state observation: {error:?}"))
        .unwrap_or_else(|| panic!("confirmed state"));
    assert_eq!(
        state.metadata().membership_epoch(),
        GroupConsumerMembershipEpoch::Consumer { member_epoch: 1 }
    );
    assert_eq!(state.metadata().group_instance_id(), Some("instance-a"));
    assert_eq!(state.metadata().group_instance_id_arc(), None);
}

#[test]
fn group_state_rejects_consumer_protocol_with_only_a_classic_epoch() {
    let (mut entry, _topic_id) = installed_modern_entry();
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("installed modern assignment"));
    let classic_assignment = LiveGroupAssignment::try_new(
        assignment.group_id(),
        assignment.member_id(),
        assignment.assignment_generation(),
        assignment.partitions().to_vec(),
    )
    .unwrap_or_else(|error| panic!("matching classic assignment: {error}"));
    let member_id = assignment.member_id();
    let member = entry
        .catalog
        .current_member()
        .cloned()
        .unwrap_or_else(|| panic!("installed modern member"));
    let installed_cycle = entry
        .catalog
        .membership_cycle()
        .unwrap_or_else(|| panic!("installed modern cycle"));
    let modern_session = entry
        .catalog
        .consumer_current
        .take()
        .unwrap_or_else(|| panic!("installed modern session"));
    entry.catalog.current = Some(CurrentGroupSession {
        member_id,
        member,
        installed_cycle,
        classic_generation: 7,
        assignment: classic_assignment,
    });
    assert_eq!(entry.protocol, GroupConsumerProtocol::Consumer);
    assert_eq!(entry.catalog.classic_generation(), Some(7));
    assert_eq!(entry.catalog.consumer_group_member_epoch(), None);

    let group_id = entry.group_id();
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);
    assert_eq!(
        registry.group_state(group_id),
        Err(GroupConsumerStateError::EntryFault)
    );

    let entry = registry
        .entries
        .first_mut()
        .unwrap_or_else(|| panic!("modern entry"));
    entry.catalog.current = None;
    entry.catalog.consumer_current = Some(modern_session);
}
