//! Rejection state, assignment fencing, and ordered recovery effect evidence.

use crate::{Deadline, GroupId, MemberId, Moment};

use super::{
    ClassicBrokerError, ClassicBrokerStage, ClassicCoordinatorRecovery, ClassicGeneration,
    ClassicGroupEffect, ClassicGroupFatal, ClassicGroupFatalReason, ClassicGroupInput,
    ClassicGroupMachine, ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatAttempt,
    ClassicHeartbeatPolicy, ClassicRejoinPolicy, MembershipCycle,
};

#[test]
fn join_coordinator_rejection_arms_rediscovery_without_reusing_a_cycle() {
    let mut machine = machine();
    let cycle = begin(&mut machine);
    let transition = machine
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now: Moment::from_tick(10),
            error: broker_error(15),
        })
        .unwrap_or_else(|error| panic!("recoverable Join rejection: {error}"));
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::ArmRejoin {
            schedule,
            coordinator: ClassicCoordinatorRecovery::Rediscover,
        }) if schedule.cycle() == cycle
            && schedule.assignment_generation().is_none()
            && schedule.due() == Deadline::from_tick(15)
    ));
    assert_eq!(machine.phase(), ClassicGroupPhase::WaitingToRejoin);
    assert_eq!(machine.active_cycle(), None);
}

#[test]
fn member_id_required_is_fatal_for_dynamic_join_versions_one_through_three() {
    let mut machine = machine();
    let cycle = begin(&mut machine);
    let transition = machine
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now: Moment::from_tick(10),
            error: broker_error(79),
        })
        .unwrap_or_else(|error| panic!("fatal Join rejection: {error}"));
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Fatal { fatal })
            if fatal.reason()
                == ClassicGroupFatalReason::Broker {
                    stage: ClassicBrokerStage::Join,
                    error: broker_error(79),
                }
    ));
    assert_eq!(machine.phase(), ClassicGroupPhase::Fatal);
}

#[test]
fn unrepresentable_rejoin_due_is_retained_as_fatal_state() {
    let mut machine = machine();
    let cycle = begin_with_deadline(&mut machine, u64::MAX);
    let transition = machine
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now: Moment::from_tick(u64::MAX - 4),
            error: broker_error(14),
        })
        .unwrap_or_else(|error| panic!("overflow is terminal state: {error}"));
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Fatal { fatal })
            if fatal.reason() == ClassicGroupFatalReason::ScheduleDeadlineOverflow
    ));
    assert_eq!(machine.phase(), ClassicGroupPhase::Fatal);
    assert_eq!(
        machine.fatal().map(ClassicGroupFatal::reason),
        Some(ClassicGroupFatalReason::ScheduleDeadlineOverflow)
    );
}

#[test]
fn sync_load_in_progress_arms_one_cycle_fenced_retained_rejoin() {
    let mut machine = machine();
    let cycle = begin(&mut machine);
    machine
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id: MemberId::try_from_raw(2).unwrap_or_else(|| panic!("nonzero member")),
            generation: ClassicGeneration::try_from_raw(7)
                .unwrap_or_else(|| panic!("nonnegative generation")),
        })
        .unwrap_or_else(|error| panic!("valid follower Join: {error}"));
    let transition = machine
        .apply(ClassicGroupInput::SyncRejected {
            cycle,
            now: Moment::from_tick(4),
            error: broker_error(14),
        })
        .unwrap_or_else(|error| panic!("recoverable Sync rejection: {error}"));
    let mut effects = transition.effects();
    assert!(matches!(
        effects.next(),
        Some(ClassicGroupEffect::ArmRejoin {
            schedule,
            coordinator: ClassicCoordinatorRecovery::Retain,
        }) if schedule.cycle() == cycle
            && schedule.assignment_generation().is_none()
            && schedule.due() == Deadline::from_tick(9)
    ));
    assert!(effects.next().is_none());
    assert_eq!(machine.phase(), ClassicGroupPhase::WaitingToRejoin);
}

#[test]
fn heartbeat_rejoin_revokes_before_arming_an_assignment_fenced_schedule() {
    let (mut machine, attempt) = stable_inflight();
    let transition = machine
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt,
            now: Moment::from_tick(4),
            error: broker_error(27),
        })
        .unwrap_or_else(|error| panic!("recoverable heartbeat rejection: {error}"));
    let mut effects = transition.effects();
    assert!(matches!(
        effects.next(),
        Some(ClassicGroupEffect::Revoke { .. })
    ));
    let Some(ClassicGroupEffect::ArmRejoin {
        schedule,
        coordinator: ClassicCoordinatorRecovery::Retain,
    }) = effects.next()
    else {
        panic!("retained-coordinator rejoin expected");
    };
    assert_eq!(schedule.cycle(), attempt.cycle());
    assert_eq!(
        schedule.assignment_generation(),
        Some(attempt.assignment_generation())
    );
    assert_eq!(schedule.due(), Deadline::from_tick(9));
    assert!(effects.next().is_none());
    assert_eq!(machine.phase(), ClassicGroupPhase::WaitingToRejoin);
    assert_eq!(machine.live_assignment(), None);
}

#[test]
fn heartbeat_rediscovery_revokes_before_one_exactly_fenced_rejoin() {
    let (mut machine, attempt) = stable_inflight();
    let transition = machine
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt,
            now: Moment::from_tick(4),
            error: broker_error(15),
        })
        .unwrap_or_else(|error| panic!("coordinator Heartbeat rejection: {error}"));
    let mut effects = transition.effects();
    assert!(matches!(
        effects.next(),
        Some(ClassicGroupEffect::Revoke { .. })
    ));
    let Some(ClassicGroupEffect::ArmRejoin {
        schedule,
        coordinator: ClassicCoordinatorRecovery::Rediscover,
    }) = effects.next()
    else {
        panic!("coordinator-rediscovery rejoin expected");
    };
    assert_eq!(schedule.cycle(), attempt.cycle());
    assert_eq!(
        schedule.assignment_generation(),
        Some(attempt.assignment_generation())
    );
    assert_eq!(schedule.due(), Deadline::from_tick(9));
    assert!(effects.next().is_none());
}

#[test]
fn fatal_heartbeat_revokes_before_retaining_the_exact_unknown_error() {
    let (mut machine, attempt) = stable_inflight();
    let error = broker_error(1234);
    let transition = machine
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt,
            now: Moment::from_tick(4),
            error,
        })
        .unwrap_or_else(|apply_error| panic!("fatal heartbeat: {apply_error}"));
    let mut effects = transition.effects();
    assert!(matches!(
        effects.next(),
        Some(ClassicGroupEffect::Revoke { .. })
    ));
    assert!(matches!(
        effects.next(),
        Some(ClassicGroupEffect::Fatal { fatal })
            if fatal.reason()
                == ClassicGroupFatalReason::Broker {
                    stage: ClassicBrokerStage::Heartbeat,
                    error,
                }
    ));
    assert!(effects.next().is_none());
    assert_eq!(machine.phase(), ClassicGroupPhase::Fatal);
    assert_eq!(machine.live_assignment(), None);
}

fn stable_inflight() -> (ClassicGroupMachine, ClassicHeartbeatAttempt) {
    let mut machine = machine();
    let cycle = begin(&mut machine);
    machine
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id: MemberId::try_from_raw(2).unwrap_or_else(|| panic!("nonzero member")),
            generation: ClassicGeneration::try_from_raw(7)
                .unwrap_or_else(|| panic!("nonnegative generation")),
        })
        .unwrap_or_else(|error| panic!("valid follower Join: {error}"));
    let install = machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(3),
            partitions: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("valid Sync: {error}"));
    let Some(ClassicGroupEffect::Install { heartbeat, .. }) = install.effects().next() else {
        panic!("Install expected");
    };
    let attempt = heartbeat.attempt();
    machine
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt,
            now: Moment::from_tick(3),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
    (machine, attempt)
}

fn begin(machine: &mut ClassicGroupMachine) -> MembershipCycle {
    begin_with_deadline(machine, 100)
}

fn begin_with_deadline(machine: &mut ClassicGroupMachine, deadline: u64) -> MembershipCycle {
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
