//! Local prepared-cycle close and mechanism-release scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicProcessingLease,
    ClassicProcessingLeaseFence, GroupId, GroupPositionBatch, GroupPositionFence,
    GroupPositionPartitionFact, Moment, NextFetchOffset,
};

use crate::clock::MonotonicClock;

use super::{
    classic_group_execution::new_classic_group_execution,
    classic_group_execution_close::ClassicGroupCloseProgress,
    classic_group_fetch::{ClassicGroupFetchFront, ClassicGroupFetchOwner},
    classic_group_owner::ClassicGroupOwner,
    classic_group_position::{ClassicGroupPositionExecutionState, test_support::completed_ready},
    classic_group_test_support,
    registry::GroupConsumerRegistry,
    registry_entry::default_classic_processing_lease_policy,
    registry_event_reconciliation_test::{
        defer_rejoin_during_reconciliation, prepared_reconciliation,
    },
    registry_membership::GroupConsumerMembershipTurn,
    session_catalog::GroupSessionCatalog,
};

#[test]
fn local_prepared_join_can_close_without_transport_or_lost_effects() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group identity"));
    let timing = ClassicGroupTiming::try_new(12_345, 54_321)
        .unwrap_or_else(|error| panic!("valid classic group timing: {error}"));
    let heartbeat = ClassicHeartbeatPolicy::try_new(1_000_000_000, 2_000_000_000)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"));
    let mut owner = ClassicGroupOwner::new(
        group_id,
        timing,
        heartbeat,
        classic_group_test_support::rejoin_policy(),
    );
    let mut catalog =
        GroupSessionCatalog::try_new(group_id, Arc::from("workers"), &[Arc::from("orders")])
            .unwrap_or_else(|error| panic!("catalog failed: {error:?}"));
    let mut execution = new_classic_group_execution();
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    execution
        .begin(&mut owner, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));

    assert_eq!(
        execution.close_if_local(
            &mut owner,
            &mut catalog,
            &mut ClassicProcessingLease::new(default_classic_processing_lease_policy()),
            &mut ClassicGroupFetchOwner::try_new()
                .unwrap_or_else(|error| panic!("Fetch owner: {error:?}")),
        ),
        Ok(ClassicGroupCloseProgress::Progress)
    );
    assert_eq!(owner.machine().phase(), ClassicGroupPhase::Closed);
    assert_eq!(execution.unsettled(), 0);
    assert!(catalog.live_assignment().is_none());
}

#[test]
fn local_close_drains_both_sides_of_pending_cooperative_reconciliation() {
    let (mut registry, group_id) = prepared_reconciliation_registry();
    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("close cooperative reconciliation: {error:?}"));

    assert_eq!(
        registry.turn_local_membership(Moment::from_tick(20)),
        Ok(GroupConsumerMembershipTurn::Progress),
        "the embedded added-position preparation transfers first"
    );
    assert!(matches!(
        registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("cooperative entry"))
            .position
            .state(),
        ClassicGroupPositionExecutionState::Prepared(_)
    ));
    assert_eq!(
        registry.turn_local_membership(Moment::from_tick(21)),
        Ok(GroupConsumerMembershipTurn::Progress),
        "the ordinary position close consumes the transferred owner"
    );
    assert_eq!(
        registry.turn_local_membership(Moment::from_tick(22)),
        Ok(GroupConsumerMembershipTurn::Progress),
        "the exact previous catalog assignment retires beside core's replacement"
    );

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("closed cooperative entry"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Closed);
    assert!(entry.classic_reconciliation.is_none());
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.position.is_dormant());
    assert!(entry.heartbeat.is_dormant());
    assert!(entry.processing_lease.active_schedule().is_none());
    assert!(entry.processing_lease.pending_expiration().is_none());
    assert_eq!(entry.fetch.machine_assignment_epoch(), None);
    assert!(entry.execution.is_idle());

    super::registry_test_support::stop_registry(&mut registry);
}

#[test]
fn local_close_drains_reconciliation_before_clearing_its_deferred_rejoin() {
    let (mut registry, group_id) = prepared_reconciliation_registry();
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("cooperative entry"));
    let schedule = defer_rejoin_during_reconciliation(entry);
    assert_eq!(entry.classic.machine().pending_rejoin(), Some(schedule));
    assert_eq!(entry.rejoin.schedule(), Some(schedule));

    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("close deferred cooperative reconciliation: {error:?}"));
    for now in 20..=22 {
        assert_eq!(
            registry.turn_local_membership(Moment::from_tick(now)),
            Ok(GroupConsumerMembershipTurn::Progress)
        );
    }

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("closed cooperative entry"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Closed);
    assert!(entry.classic_reconciliation.is_none());
    assert!(entry.rejoin.is_dormant());
    assert!(entry.catalog.live_assignment().is_none());
    assert_eq!(entry.fetch.machine_assignment_epoch(), None);

    super::registry_test_support::stop_registry(&mut registry);
}

#[test]
fn driver_shutdown_settles_revocation_then_drains_pending_cooperative_reconciliation() {
    let (mut registry, group_id) = prepared_reconciliation_registry();
    registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("cooperative entry"))
        .classic_reconciliation
        .as_mut()
        .unwrap_or_else(|| panic!("pending cooperative reconciliation"))
        .confirm_sync();
    assert_eq!(
        registry.stage_one_classic_group_reconciliation(Moment::from_tick(20)),
        Ok(super::classic_group_reconciliation_turn::ClassicGroupReconciliationTurn::Progress)
    );
    assert!(
        !registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("staged cooperative entry"))
            .revocation
            .is_dormant()
    );

    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover cooperative close: {error}"));
    assert!(registry.entry(group_id).is_none());
    let fetch = registry
        .fetch_shutdown_recovery(group_id)
        .unwrap_or_else(|| panic!("cooperative Fetch shutdown recovery"));
    assert_eq!(fetch.activation(), None);
    assert_eq!(fetch.machine_assignment(), None);
    assert_eq!(
        fetch.effects(),
        3,
        "one paused partition and two previous-assignment revokes remain observable to shutdown recovery"
    );
    assert_eq!(
        fetch.prepared(),
        2,
        "both previously prepared Fetch requests remain explicit in post-driver recovery"
    );

    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish cooperative shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join cooperative notifier: {error}"));
    assert!(registry.entries.is_empty());
}

fn prepared_reconciliation_registry() -> (GroupConsumerRegistry, GroupId) {
    let mut entry = prepared_reconciliation();
    let group_id = entry.group_id();
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("previous catalog assignment"));
    let cycle = entry
        .catalog
        .membership_cycle()
        .unwrap_or_else(|| panic!("previous catalog cycle"));
    let fence = GroupPositionFence::new(
        group_id,
        cycle,
        assignment.member_id(),
        assignment.assignment_generation(),
    );
    entry
        .processing_lease
        .prepare_activation(
            ClassicProcessingLeaseFence::new(group_id, cycle, assignment.assignment_generation()),
            Moment::from_tick(10),
        )
        .unwrap_or_else(|error| panic!("previous processing lease: {error:?}"))
        .commit();
    let position_facts = assignment
        .partitions()
        .iter()
        .copied()
        .map(|partition| {
            GroupPositionPartitionFact::committed(
                partition,
                NextFetchOffset::try_from_raw(17).unwrap_or_else(|| panic!("positive next offset")),
            )
        })
        .collect();
    entry
        .fetch
        .try_activate(
            completed_ready(
                fence,
                Moment::from_tick(11),
                GroupPositionBatch::new(0, position_facts),
            ),
            fence,
        )
        .unwrap_or_else(|_error| panic!("previous Fetch activation"));
    let fetch_clock = MonotonicClock::new();
    for _effect in 0..assignment.partitions().len() {
        assert_eq!(
            entry
                .fetch
                .interpret_front_effect(&entry.catalog, &fetch_clock),
            ClassicGroupFetchFront::Interpreted,
            "previous Fetch activation effects drain before cooperative fencing"
        );
    }
    assert_eq!(
        entry
            .fetch
            .interpret_front_effect(&entry.catalog, &fetch_clock),
        ClassicGroupFetchFront::Idle
    );
    let heartbeat = entry
        .classic_reconciliation
        .as_ref()
        .unwrap_or_else(|| panic!("pending cooperative reconciliation"))
        .reconciliation()
        .heartbeat();
    entry
        .heartbeat
        .prepare_install(heartbeat)
        .unwrap_or_else(|error| panic!("replacement heartbeat: {error:?}"))
        .commit();

    let retained_bytes = entry.group_bytes();
    let mut registry = GroupConsumerRegistry::start()
        .unwrap_or_else(|error| panic!("cooperative registry: {error}"));
    registry.retained_group_bytes = retained_bytes;
    registry.next_group_id = GroupId::try_from_raw(2);
    registry.entries.push(entry);
    (registry, group_id)
}
