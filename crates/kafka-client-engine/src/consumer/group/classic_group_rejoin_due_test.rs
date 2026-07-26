//! Due selection, fixed-epoch staging, stale fences, and bounded turn scenarios.

use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupInput, ClassicGroupPhase, MembershipCycle, Moment,
};

use crate::clock::MonotonicClock;

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::PreparedClassicGroupJoin,
    classic_group_rejoin_due::ClassicGroupRejoinDueTurn,
    classic_group_rejoin_test_support::{arm_rejoin, entry_mut, reject_join},
    registry_test_support::{deadline, register, started_registry, stop_registry},
};

#[test]
fn early_observation_waits_and_exact_due_stages_one_fresh_join() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let schedule = arm_rejoin(&mut registry, group_id, 10);
    let clock = MonotonicClock::new();

    assert_eq!(
        registry.prepare_one_classic_rejoin(Moment::from_tick(schedule.due().tick() - 1), &clock,),
        Ok(ClassicGroupRejoinDueTurn::Idle)
    );
    assert_eq!(
        registry.prepare_one_classic_rejoin(Moment::from_tick(schedule.due().tick()), &clock),
        Ok(ClassicGroupRejoinDueTurn::Progress)
    );

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let prepared = entry
        .execution
        .prepared_join()
        .unwrap_or_else(|| panic!("fresh prepared Join expected"));
    assert_eq!(
        prepared.cycle(),
        schedule
            .cycle()
            .checked_next()
            .unwrap_or_else(|| panic!("next cycle expected"))
    );
    assert_eq!(
        prepared.deadline(),
        clock
            .operation_deadline(
                entry
                    .classic
                    .machine()
                    .deadline()
                    .unwrap_or_else(|| panic!("fresh core deadline expected")),
            )
            .unwrap_or_else(|error| panic!("fixed-epoch mapping failed: {error}"))
    );
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Joining);
    assert!(entry.rejoin.is_dormant());
    stop_registry(&mut registry);
}

#[test]
fn stale_engine_schedule_cannot_advance_the_new_core_schedule() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let stale = arm_rejoin(&mut registry, group_id, 10);
    let entry = entry_mut(&mut registry, group_id);
    let transition = entry
        .classic
        .apply(ClassicGroupInput::RejoinDue {
            schedule: stale,
            now: Moment::from_tick(stale.due().tick()),
        })
        .unwrap_or_else(|error| panic!("first rejoin failed: {error}"));
    let cycle = transition
        .into_effects()
        .find_map(|effect| match effect {
            ClassicGroupEffect::Join { cycle, .. } => Some(cycle),
            _ => None,
        })
        .unwrap_or_else(|| panic!("fresh Join expected"));
    let fresh = reject_join(&mut entry.classic, cycle, stale.due().tick() + 1);

    assert_eq!(
        registry.prepare_one_classic_rejoin(
            Moment::from_tick(fresh.due().tick()),
            &MonotonicClock::new(),
        ),
        Err(ClassicGroupExecutionError::RejoinState)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.rejoin.schedule(), Some(stale));
    assert_eq!(entry.classic.machine().pending_rejoin(), Some(fresh));
    stop_registry(&mut registry);
}

#[test]
fn occupied_execution_preserves_the_due_schedule_without_mutating_core() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let schedule = arm_rejoin(&mut registry, group_id, 10);
    let entry = entry_mut(&mut registry, group_id);
    let occupied = PreparedClassicGroupJoin::new(
        group_id,
        MembershipCycle::initial(),
        kafka_client_core::ClassicProtocol::Range,
        entry.classic.machine().timing(),
        deadline(99),
    );
    entry
        .execution
        .stage_rejoin_join(occupied)
        .unwrap_or_else(|(error, _owner)| panic!("occupy execution failed: {error:?}"));

    assert_eq!(
        registry.prepare_one_classic_rejoin(
            Moment::from_tick(schedule.due().tick()),
            &MonotonicClock::new(),
        ),
        Err(ClassicGroupExecutionError::RejoinState)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.rejoin.schedule(), Some(schedule));
    assert_eq!(entry.classic.machine().pending_rejoin(), Some(schedule));
    stop_registry(&mut registry);
}

#[test]
fn one_due_schedule_is_staged_per_turn() {
    let mut registry = started_registry();
    let first = register(&mut registry, "first");
    let second = register(&mut registry, "second");
    let first_schedule = arm_rejoin(&mut registry, first, 10);
    let second_schedule = arm_rejoin(&mut registry, second, 10);
    let now = Moment::from_tick(
        first_schedule
            .due()
            .tick()
            .max(second_schedule.due().tick()),
    );

    assert_eq!(
        registry.prepare_one_classic_rejoin(now, &MonotonicClock::new()),
        Ok(ClassicGroupRejoinDueTurn::Progress)
    );
    assert!(
        registry
            .entry(first)
            .unwrap_or_else(|| panic!("first entry expected"))
            .execution
            .prepared_join()
            .is_some()
    );
    assert_eq!(
        registry
            .entry(second)
            .unwrap_or_else(|| panic!("second entry expected"))
            .rejoin
            .schedule(),
        Some(second_schedule)
    );
    stop_registry(&mut registry);
}

#[test]
fn rediscovery_hides_an_elapsed_rejoin_until_driver_terminal_permission() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let schedule = arm_rejoin(&mut registry, group_id, 10);
    let entry = entry_mut(&mut registry, group_id);
    entry
        .rediscovery
        .prepare_rediscovery_install()
        .unwrap_or_else(|error| panic!("rediscovery install failed: {error:?}"))
        .commit();
    entry
        .rediscovery
        .confirm_rediscovery_transfer()
        .unwrap_or_else(|error| panic!("route transfer failed: {error:?}"));

    assert_eq!(registry.membership_next_deadline(), None);
    assert_eq!(
        registry.prepare_one_classic_rejoin(
            Moment::from_tick(schedule.due().tick()),
            &MonotonicClock::new(),
        ),
        Ok(ClassicGroupRejoinDueTurn::Idle)
    );

    entry_mut(&mut registry, group_id)
        .rediscovery
        .permit_rejoin()
        .unwrap_or_else(|error| panic!("driver terminal permission failed: {error:?}"));
    assert_eq!(registry.membership_next_deadline(), Some(schedule.due()));
    assert_eq!(
        registry.prepare_one_classic_rejoin(
            Moment::from_tick(schedule.due().tick()),
            &MonotonicClock::new(),
        ),
        Ok(ClassicGroupRejoinDueTurn::Progress)
    );
    stop_registry(&mut registry);
}
