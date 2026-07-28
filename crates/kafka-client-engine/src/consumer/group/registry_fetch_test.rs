//! Registry-host transfer, bounded-turn, deadline, and ownership accounting scenarios.

use kafka_client_core::{
    GroupId, GroupPositionBatch, GroupPositionFence, GroupPositionPartitionFact, Moment,
    NextFetchOffset,
};

use crate::{
    clock::MonotonicClock,
    consumer::assigned_owner_test::{driver, shutdown},
    driver::DriverOwner,
};

use super::{
    classic_group_position::{ClassicGroupPositionExecutionState, test_support::completed_ready},
    registry::GroupConsumerRegistry,
    registry_fetch::GroupConsumerFetchTurn,
    registry_test_support::{install_session, register, started_registry},
};

const FETCH_ATTEMPT_TICKS: u64 = 30_000_000_000;

#[test]
fn confirmed_position_transfers_once_before_one_bounded_fetch_action() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "group-a");
    install_session(&mut registry, group_id);
    install_completed_position(&mut registry, group_id, 17);
    let clock = MonotonicClock::new();
    let mut driver = driver();

    assert_eq!(
        registry.turn_fetch(&clock, &driver),
        Ok(GroupConsumerFetchTurn::Progress)
    );
    let current = registered_entry(&registry, group_id);
    let assignment_epoch = current
        .fetch
        .machine_assignment_epoch()
        .unwrap_or_else(|| panic!("Fetch assignment expected"));
    assert!(matches!(
        current.position.state(),
        ClassicGroupPositionExecutionState::Dormant
    ));
    assert!(current.fetch.activation().is_some());
    assert_eq!(registry.fetch_unsettled(), 3);
    assert_eq!(registry.fetch_next_deadline(), None);

    assert_eq!(
        registry.turn_fetch(&clock, &driver),
        Ok(GroupConsumerFetchTurn::Progress)
    );
    let current = registered_entry(&registry, group_id);
    assert_eq!(
        current.fetch.machine_assignment_epoch(),
        Some(assignment_epoch)
    );
    assert!(matches!(
        current.position.state(),
        ClassicGroupPositionExecutionState::Dormant
    ));
    assert!(registry.fetch_next_deadline().is_some());
    assert_eq!(registry.fetch_unsettled(), 3);

    stop_after_fetch_recovery(&mut registry, &mut driver, group_id);
}

#[test]
fn registry_aggregates_fetch_deadline_and_unsettled_ownership() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "group-a");
    install_session(&mut registry, group_id);
    install_completed_position(&mut registry, group_id, 23);
    let clock = MonotonicClock::new();
    let mut driver = driver();

    assert_eq!(registry.fetch_unsettled(), 0);
    assert_eq!(registry.fetch_next_deadline(), None);
    assert_eq!(
        registry.turn_fetch(&clock, &driver),
        Ok(GroupConsumerFetchTurn::Progress)
    );
    assert_eq!(registry.fetch_unsettled(), 3);
    assert_eq!(registry.fetch_next_deadline(), None);

    let before = clock
        .now()
        .unwrap_or_else(|error| panic!("clock before Fetch preparation: {error}"));
    assert_eq!(
        registry.turn_fetch(&clock, &driver),
        Ok(GroupConsumerFetchTurn::Progress)
    );
    let after = clock
        .now()
        .unwrap_or_else(|error| panic!("clock after Fetch preparation: {error}"));
    let deadline = registry
        .fetch_next_deadline()
        .unwrap_or_else(|| panic!("prepared Fetch deadline expected"));
    assert!(deadline.tick() >= before.tick().saturating_add(FETCH_ATTEMPT_TICKS));
    assert!(deadline.tick() <= after.tick().saturating_add(FETCH_ATTEMPT_TICKS));
    assert_eq!(registry.fetch_unsettled(), 3);

    stop_after_fetch_recovery(&mut registry, &mut driver, group_id);
}

fn install_completed_position(
    registry: &mut GroupConsumerRegistry,
    group_id: GroupId,
    next_offset: i64,
) {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group expected"));
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment expected"));
    let partition = assignment
        .partitions()
        .first()
        .copied()
        .unwrap_or_else(|| panic!("assigned partition expected"));
    let fence = GroupPositionFence::new(
        assignment.group_id(),
        entry
            .classic
            .machine()
            .active_cycle()
            .unwrap_or_else(|| panic!("active membership cycle expected")),
        assignment.member_id(),
        assignment.assignment_generation(),
    );
    let completed = completed_ready(
        fence,
        Moment::from_tick(41),
        GroupPositionBatch::new(
            0,
            vec![GroupPositionPartitionFact::committed(
                partition,
                NextFetchOffset::try_from_raw(next_offset)
                    .unwrap_or_else(|| panic!("next Fetch offset")),
            )],
        ),
    );
    entry
        .position
        .set(ClassicGroupPositionExecutionState::Complete(completed));
}

fn registered_entry(
    registry: &GroupConsumerRegistry,
    group_id: GroupId,
) -> &super::registry_entry::GroupConsumerEntry {
    registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered group expected"))
}

fn stop_after_fetch_recovery(
    registry: &mut GroupConsumerRegistry,
    driver: &mut DriverOwner,
    group_id: GroupId,
) {
    shutdown(driver);
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("group Fetch recovery: {error}"));
    let recovery = registry
        .fetch_shutdown_recovery(group_id)
        .unwrap_or_else(|| panic!("group Fetch shutdown recovery expected"));
    assert_eq!(recovery.activation(), None);
    assert_eq!(recovery.machine_assignment(), None);
    assert_eq!(recovery.effects(), 1);
    assert_eq!(recovery.prepared(), 1);
    assert_eq!(recovery.fetch_retained(), (0, 0, 0));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish recovered registry: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("group Fetch notifier join: {error}"));
}
