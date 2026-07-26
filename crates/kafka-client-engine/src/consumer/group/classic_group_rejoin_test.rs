//! Exact schedule installation, due visibility, and stale-clear scenarios.

use kafka_client_core::{
    ClassicBrokerError, ClassicGroupEffect, ClassicGroupInput, Deadline, Moment,
};

use super::{
    classic_group_owner::ClassicGroupOwner,
    classic_group_rejoin::{ClassicGroupRejoinError, ClassicGroupRejoinExecution},
    classic_group_test_support,
};

#[test]
fn install_is_linear_and_exposes_the_exact_due_deadline() {
    let (schedule, _owner) = rejected_join();
    let mut execution = ClassicGroupRejoinExecution::new();
    execution
        .prepare_rejoin_install(schedule)
        .unwrap_or_else(|error| panic!("schedule install failed: {error:?}"))
        .commit();

    assert_eq!(execution.schedule(), Some(schedule));
    assert_eq!(execution.next_deadline(), Some(schedule.due()));
    assert_eq!(execution.unsettled(), 1);
    assert_eq!(
        execution.prepare_rejoin_install(schedule).err(),
        Some(ClassicGroupRejoinError::Occupied)
    );
}

#[test]
fn stale_clear_preserves_the_exact_waiting_schedule() {
    let (schedule, mut owner) = rejected_join();
    let mut execution = ClassicGroupRejoinExecution::new();
    execution
        .prepare_rejoin_install(schedule)
        .unwrap_or_else(|error| panic!("schedule install failed: {error:?}"))
        .commit();
    let stale = next_rejected_join(&mut owner, schedule.due());

    assert_eq!(
        execution.clear_rejoin_exact(stale),
        Err(ClassicGroupRejoinError::ScheduleMismatch)
    );
    assert_eq!(execution.schedule(), Some(schedule));
}

#[test]
fn exact_clear_returns_the_owner_to_dormant() {
    let (schedule, _owner) = rejected_join();
    let mut execution = ClassicGroupRejoinExecution::new();
    execution
        .prepare_rejoin_install(schedule)
        .unwrap_or_else(|error| panic!("schedule install failed: {error:?}"))
        .commit();

    execution
        .clear_rejoin_exact(schedule)
        .unwrap_or_else(|error| panic!("exact clear failed: {error:?}"));

    assert!(execution.is_dormant());
    assert_eq!(execution.next_deadline(), None);
    assert_eq!(execution.unsettled(), 0);
}

fn rejected_join() -> (kafka_client_core::ClassicRejoinSchedule, ClassicGroupOwner) {
    let mut owner = ClassicGroupOwner::new(
        kafka_client_core::GroupId::try_from_raw(1)
            .unwrap_or_else(|| panic!("nonzero group identity")),
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    let _cycle = classic_group_test_support::begin(&mut owner);
    let schedule = reject_active_join(&mut owner, Moment::from_tick(10));
    (schedule, owner)
}

fn next_rejected_join(
    owner: &mut ClassicGroupOwner,
    due: Deadline,
) -> kafka_client_core::ClassicRejoinSchedule {
    let schedule = owner
        .machine()
        .pending_rejoin()
        .unwrap_or_else(|| panic!("pending rejoin expected"));
    let transition = owner
        .apply(ClassicGroupInput::RejoinDue {
            schedule,
            now: Moment::from_tick(due.tick()),
        })
        .unwrap_or_else(|error| panic!("due rejoin failed: {error}"));
    let Some(ClassicGroupEffect::Join { cycle, .. }) = transition.effects().next() else {
        panic!("fresh Join expected");
    };
    owner
        .apply(ClassicGroupInput::JoinRejected {
            cycle: *cycle,
            now: Moment::from_tick(due.tick().saturating_add(1)),
            error: broker_error(),
        })
        .unwrap_or_else(|error| panic!("second Join rejection failed: {error}"))
        .into_effects()
        .find_map(|effect| match effect {
            ClassicGroupEffect::ArmRejoin { schedule, .. } => Some(schedule),
            _ => None,
        })
        .unwrap_or_else(|| panic!("second schedule expected"))
}

fn reject_active_join(
    owner: &mut ClassicGroupOwner,
    now: Moment,
) -> kafka_client_core::ClassicRejoinSchedule {
    let cycle = owner
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle expected"));
    owner
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now,
            error: broker_error(),
        })
        .unwrap_or_else(|error| panic!("Join rejection failed: {error}"))
        .into_effects()
        .find_map(|effect| match effect {
            ClassicGroupEffect::ArmRejoin { schedule, .. } => Some(schedule),
            _ => None,
        })
        .unwrap_or_else(|| panic!("rejoin schedule expected"))
}

fn broker_error() -> ClassicBrokerError {
    ClassicBrokerError::try_from_code(14).unwrap_or_else(|| panic!("nonzero broker error"))
}
