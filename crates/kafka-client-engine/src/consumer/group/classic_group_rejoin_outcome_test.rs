//! Close precedence, exact fatal settlement, and post-core freeze scenarios.

use kafka_client_core::{
    ClassicGroupFatal, ClassicGroupFatalReason, ClassicGroupInput, ClassicGroupPhase, Deadline,
    Moment,
};

use crate::clock::MonotonicClock;

use super::{
    classic_group_rejoin_due::ClassicGroupRejoinDueTurn,
    classic_group_rejoin_test_support::{arm_rejoin, entry_mut, reject_join},
    classic_group_test_support,
    registry_membership::GroupConsumerMembershipTurn,
    registry_test_support::{register, started_registry, stop_registry},
};

#[test]
fn close_at_due_clears_the_timer_before_rejoin_can_run() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let schedule = arm_rejoin(&mut registry, group_id, 10);
    assert_eq!(registry.membership_unsettled(), 1);
    assert_eq!(registry.membership_next_deadline(), Some(schedule.due()));
    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("close failed: {error:?}"));

    assert_eq!(
        registry.turn_local_membership(Moment::from_tick(schedule.due().tick())),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Closed);
    assert!(entry.rejoin.is_dormant());
    assert!(entry.execution.is_idle());
    assert_eq!(registry.membership_unsettled(), 0);
    stop_registry(&mut registry);
}

#[test]
fn attempt_deadline_overflow_settles_as_exact_core_fatal_policy() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let entry = entry_mut(&mut registry, group_id);
    entry
        .classic
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(u64::MAX),
        })
        .unwrap_or_else(|error| panic!("begin failed: {error}"));
    let cycle = entry
        .classic
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle expected"));
    let rejected_at = u64::MAX - classic_group_test_support::rejoin_policy().backoff_ticks();
    let schedule = reject_join(&mut entry.classic, cycle, rejected_at);
    entry
        .rejoin
        .prepare_rejoin_install(schedule)
        .unwrap_or_else(|error| panic!("rejoin install failed: {error:?}"))
        .commit();

    assert_eq!(schedule.due(), Deadline::from_tick(u64::MAX));
    assert_eq!(
        registry.prepare_one_classic_rejoin(Moment::from_tick(u64::MAX), &MonotonicClock::new()),
        Ok(ClassicGroupRejoinDueTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Fatal);
    assert_eq!(
        entry
            .classic
            .machine()
            .fatal()
            .map(ClassicGroupFatal::reason),
        Some(ClassicGroupFatalReason::AttemptDeadlineOverflow)
    );
    assert!(entry.rejoin.is_dormant());
    assert!(entry.execution.is_idle());
    assert!(entry.fault.is_none());
    stop_registry(&mut registry);
}
