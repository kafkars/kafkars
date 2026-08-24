//! Exact-boundary rejection precedence over Join, Sync, and Heartbeat policy.

use crate::{Deadline, GroupId, MemberId, Moment};

use super::{
    ClassicBrokerError, ClassicGeneration, ClassicGroupEffect, ClassicGroupErrorKind,
    ClassicGroupFatalReason, ClassicGroupInput, ClassicGroupMachine, ClassicGroupPhase,
    ClassicGroupTiming, ClassicHeartbeatAttempt, ClassicHeartbeatPolicy, ClassicRejoinPolicy,
    MembershipCycle,
};

#[test]
fn join_rejection_uses_policy_before_but_loses_at_the_cycle_deadline() {
    let mut early = machine();
    let cycle = begin(&mut early);
    let transition = reject_join(&mut early, cycle, 99, 14);
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::ArmRejoin { .. })
    ));
    assert_eq!(early.phase(), ClassicGroupPhase::WaitingToRejoin);

    let mut exact = machine();
    let cycle = begin(&mut exact);
    let transition = reject_join(&mut exact, cycle, 100, 14);
    assert_eq!(transition.effects().count(), 0);
    assert_lost_without_recovery(&exact);
}

#[test]
fn sync_rejection_uses_policy_before_but_loses_at_the_cycle_deadline() {
    let mut early = machine();
    let cycle = begin(&mut early);
    follow(&mut early, cycle);
    let transition = reject_sync(&mut early, cycle, 99, 1234);
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Fatal { fatal })
            if fatal.reason()
                == ClassicGroupFatalReason::Broker {
                    stage: super::ClassicBrokerStage::Sync,
                    error: broker_error(1234),
                }
    ));
    assert_eq!(early.phase(), ClassicGroupPhase::Fatal);

    let mut exact = machine();
    let cycle = begin(&mut exact);
    follow(&mut exact, cycle);
    let transition = reject_sync(&mut exact, cycle, 100, 1234);
    assert_eq!(transition.effects().count(), 0);
    assert_lost_without_recovery(&exact);
}

#[test]
fn heartbeat_rejection_stops_at_deadline_while_coordinator_loss_starts_a_fresh_rejoin() {
    let (mut early, attempt, deadline) = stable_inflight();
    let transition = reject_heartbeat(&mut early, attempt, deadline.tick() - 1, 15);
    let mut effects = transition.effects();
    assert!(matches!(
        effects.next(),
        Some(ClassicGroupEffect::Revoke { .. })
    ));
    assert!(matches!(
        effects.next(),
        Some(ClassicGroupEffect::ArmRejoin { .. })
    ));
    assert!(effects.next().is_none());
    assert_eq!(early.phase(), ClassicGroupPhase::WaitingToRejoin);

    let (mut exact, attempt, deadline) = stable_inflight();
    let transition = reject_heartbeat(&mut exact, attempt, deadline.tick(), 15);
    let mut effects = transition.effects();
    assert!(matches!(
        effects.next(),
        Some(ClassicGroupEffect::Revoke { .. })
    ));
    assert!(effects.next().is_none());
    assert_lost_without_recovery(&exact);

    let (mut recovered, attempt, deadline) = stable_inflight();
    let transition = recovered
        .apply(ClassicGroupInput::HeartbeatCoordinatorLost {
            attempt,
            now: Moment::from_tick(deadline.tick() + 1),
        })
        .unwrap_or_else(|error| panic!("expired-attempt coordinator loss: {error}"));
    let mut effects = transition.effects();
    assert!(matches!(
        effects.next(),
        Some(ClassicGroupEffect::Revoke { .. })
    ));
    assert!(matches!(
        effects.next(),
        Some(ClassicGroupEffect::ArmRejoin { .. })
    ));
    assert!(effects.next().is_none());
    assert_eq!(recovered.phase(), ClassicGroupPhase::WaitingToRejoin);
}

#[test]
fn stale_phase_cycle_and_attempt_rejections_do_not_spend_deadline_ownership() {
    let mut joining = machine();
    let cycle = begin(&mut joining);
    let stale_cycle = cycle
        .checked_next()
        .unwrap_or_else(|| panic!("second cycle"));
    let error = reject_join_error(&mut joining, stale_cycle, 100, 14);
    assert_eq!(error, ClassicGroupErrorKind::CycleMismatch);
    assert_eq!(joining.phase(), ClassicGroupPhase::Joining);
    assert_eq!(joining.active_cycle(), Some(cycle));
    assert_eq!(joining.deadline(), Some(Deadline::from_tick(100)));

    let error = reject_sync_error(&mut joining, cycle, 100, 14);
    assert_eq!(error, ClassicGroupErrorKind::InvalidPhase);
    assert_eq!(joining.phase(), ClassicGroupPhase::Joining);
    assert_eq!(joining.deadline(), Some(Deadline::from_tick(100)));

    let (mut stable, attempt, deadline) = stable_inflight();
    let stale_attempt = attempt
        .checked_next()
        .unwrap_or_else(|| panic!("second heartbeat attempt"));
    let error = reject_heartbeat_error(&mut stable, stale_attempt, deadline.tick(), 15);
    assert_eq!(error, ClassicGroupErrorKind::HeartbeatMismatch);
    assert_eq!(stable.phase(), ClassicGroupPhase::Stable);
    assert!(stable.live_assignment().is_some());
    assert_eq!(stable.pending_rejoin(), None);
}

fn reject_join(
    machine: &mut ClassicGroupMachine,
    cycle: MembershipCycle,
    now: u64,
    code: i16,
) -> super::ClassicGroupTransition {
    machine
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now: Moment::from_tick(now),
            error: broker_error(code),
        })
        .unwrap_or_else(|error| panic!("valid Join rejection: {error}"))
}

fn reject_sync(
    machine: &mut ClassicGroupMachine,
    cycle: MembershipCycle,
    now: u64,
    code: i16,
) -> super::ClassicGroupTransition {
    machine
        .apply(ClassicGroupInput::SyncRejected {
            cycle,
            now: Moment::from_tick(now),
            error: broker_error(code),
        })
        .unwrap_or_else(|error| panic!("valid Sync rejection: {error}"))
}

fn reject_heartbeat(
    machine: &mut ClassicGroupMachine,
    attempt: ClassicHeartbeatAttempt,
    now: u64,
    code: i16,
) -> super::ClassicGroupTransition {
    machine
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt,
            now: Moment::from_tick(now),
            error: broker_error(code),
        })
        .unwrap_or_else(|error| panic!("valid Heartbeat rejection: {error}"))
}

fn reject_join_error(
    machine: &mut ClassicGroupMachine,
    cycle: MembershipCycle,
    now: u64,
    code: i16,
) -> ClassicGroupErrorKind {
    machine
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now: Moment::from_tick(now),
            error: broker_error(code),
        })
        .err()
        .unwrap_or_else(|| panic!("stale Join rejection must fail"))
        .kind()
}

fn reject_sync_error(
    machine: &mut ClassicGroupMachine,
    cycle: MembershipCycle,
    now: u64,
    code: i16,
) -> ClassicGroupErrorKind {
    machine
        .apply(ClassicGroupInput::SyncRejected {
            cycle,
            now: Moment::from_tick(now),
            error: broker_error(code),
        })
        .err()
        .unwrap_or_else(|| panic!("out-of-phase Sync rejection must fail"))
        .kind()
}

fn reject_heartbeat_error(
    machine: &mut ClassicGroupMachine,
    attempt: ClassicHeartbeatAttempt,
    now: u64,
    code: i16,
) -> ClassicGroupErrorKind {
    machine
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt,
            now: Moment::from_tick(now),
            error: broker_error(code),
        })
        .err()
        .unwrap_or_else(|| panic!("stale Heartbeat rejection must fail"))
        .kind()
}

fn stable_inflight() -> (ClassicGroupMachine, ClassicHeartbeatAttempt, Deadline) {
    let mut machine = machine();
    let cycle = begin(&mut machine);
    follow(&mut machine, cycle);
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
    let submit = machine
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt,
            now: Moment::from_tick(3),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
    let Some(ClassicGroupEffect::SubmitHeartbeat { deadline, .. }) = submit.effects().next() else {
        panic!("SubmitHeartbeat expected");
    };
    (machine, attempt, *deadline)
}

fn begin(machine: &mut ClassicGroupMachine) -> MembershipCycle {
    machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("valid Begin: {error}"));
    machine
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle"))
}

fn follow(machine: &mut ClassicGroupMachine, cycle: MembershipCycle) {
    machine
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id: MemberId::try_from_raw(2).unwrap_or_else(|| panic!("nonzero member")),
            generation: ClassicGeneration::try_from_raw(7)
                .unwrap_or_else(|| panic!("nonnegative generation")),
        })
        .unwrap_or_else(|error| panic!("valid follower Join: {error}"));
}

fn assert_lost_without_recovery(machine: &ClassicGroupMachine) {
    assert_eq!(machine.phase(), ClassicGroupPhase::Lost);
    assert_eq!(machine.pending_rejoin(), None);
    assert_eq!(machine.fatal(), None);
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
