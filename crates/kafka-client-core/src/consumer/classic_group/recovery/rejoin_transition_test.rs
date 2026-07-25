//! Exact due-fence and fresh internal rejoin deadline evidence.

use crate::{Deadline, GroupId, Moment};

use super::{
    ClassicBrokerError, ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupFatal,
    ClassicGroupFatalReason, ClassicGroupInput, ClassicGroupMachine, ClassicGroupPhase,
    ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicRejoinPolicy, ClassicRejoinSchedule,
    MembershipCycle,
};

#[test]
fn exact_due_schedule_starts_a_fresh_cycle_and_fresh_absolute_deadline() {
    let mut machine = machine();
    let first = begin(&mut machine, 100);
    let schedule = reject_join(&mut machine, first, 10, 14);
    assert_eq!(schedule.due(), Deadline::from_tick(15));
    assert_eq!(machine.phase(), ClassicGroupPhase::WaitingToRejoin);

    let early = machine
        .apply(ClassicGroupInput::RejoinDue {
            schedule,
            now: Moment::from_tick(14),
        })
        .err()
        .unwrap_or_else(|| panic!("early schedule must reject"));
    assert_eq!(early.kind(), ClassicGroupErrorKind::DeadlineNotElapsed);
    assert_eq!(machine.pending_rejoin(), Some(schedule));

    let transition = machine
        .apply(ClassicGroupInput::RejoinDue {
            schedule,
            now: Moment::from_tick(15),
        })
        .unwrap_or_else(|error| panic!("due rejoin: {error}"));
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Join {
            cycle,
            deadline,
            ..
        }) if cycle.get() == 2 && *deadline == Deadline::from_tick(65)
    ));
    assert_eq!(machine.phase(), ClassicGroupPhase::Joining);
    assert_eq!(machine.pending_rejoin(), None);
}

#[test]
fn stale_schedule_rejects_without_displacing_the_pending_fence() {
    let mut machine = machine();
    let cycle = begin(&mut machine, 100);
    let schedule = reject_join(&mut machine, cycle, 10, 14);
    let stale =
        ClassicRejoinSchedule::new(cycle, None, Deadline::from_tick(schedule.due().tick() + 1));
    let error = machine
        .apply(ClassicGroupInput::RejoinDue {
            schedule: stale,
            now: Moment::from_tick(20),
        })
        .err()
        .unwrap_or_else(|| panic!("stale schedule must reject"));
    assert_eq!(error.kind(), ClassicGroupErrorKind::RejoinMismatch);
    assert_eq!(machine.pending_rejoin(), Some(schedule));
}

#[test]
fn due_rejoin_deadline_overflow_enters_retained_fatal_state() {
    let mut machine = machine();
    let cycle = begin(&mut machine, 100);
    let schedule = reject_join(&mut machine, cycle, u64::MAX - 5, 14);
    let transition = machine
        .apply(ClassicGroupInput::RejoinDue {
            schedule,
            now: Moment::from_tick(u64::MAX),
        })
        .unwrap_or_else(|error| panic!("overflow is terminal state: {error}"));
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Fatal { fatal })
            if fatal.reason() == ClassicGroupFatalReason::AttemptDeadlineOverflow
    ));
    assert_eq!(machine.phase(), ClassicGroupPhase::Fatal);
    assert_eq!(
        machine.fatal().map(ClassicGroupFatal::reason),
        Some(ClassicGroupFatalReason::AttemptDeadlineOverflow)
    );
}

fn reject_join(
    machine: &mut ClassicGroupMachine,
    cycle: MembershipCycle,
    now: u64,
    code: i16,
) -> ClassicRejoinSchedule {
    let transition = machine
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now: Moment::from_tick(now),
            error: broker_error(code),
        })
        .unwrap_or_else(|error| panic!("valid Join rejection: {error}"));
    let Some(ClassicGroupEffect::ArmRejoin { schedule, .. }) = transition.effects().next() else {
        panic!("ArmRejoin expected");
    };
    *schedule
}

fn begin(machine: &mut ClassicGroupMachine, deadline: u64) -> MembershipCycle {
    machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(deadline),
        })
        .unwrap_or_else(|error| panic!("valid Begin: {error}"));
    machine
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle"))
}

fn machine() -> ClassicGroupMachine {
    ClassicGroupMachine::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group")),
        ClassicGroupTiming::try_new(10_000, 30_000)
            .unwrap_or_else(|error| panic!("valid timing: {error}")),
        ClassicHeartbeatPolicy::try_new(10, 20)
            .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}")),
        ClassicRejoinPolicy::try_new(5, 50)
            .unwrap_or_else(|error| panic!("valid rejoin policy: {error:?}")),
    )
}

fn broker_error(code: i16) -> ClassicBrokerError {
    ClassicBrokerError::try_from_code(code).unwrap_or_else(|| panic!("nonzero error"))
}
