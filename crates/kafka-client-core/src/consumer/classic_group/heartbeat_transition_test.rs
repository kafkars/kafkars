//! Assignment-fenced classic heartbeat lifecycle evidence.

use crate::{Deadline, GroupId, MemberId, Moment};

use super::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupInput,
    ClassicGroupMachine, ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatAttempt,
    ClassicHeartbeatPolicy, MembershipCycle,
};

#[test]
fn sync_success_installs_one_assignment_and_arms_its_exact_first_heartbeat() {
    let (machine, cycle, effect) = install(heartbeat_policy());
    let ClassicGroupEffect::Install {
        assignment,
        classic_generation,
        heartbeat,
    } = effect
    else {
        panic!("Install effect expected");
    };

    assert_eq!(machine.phase(), ClassicGroupPhase::Stable);
    assert_eq!(assignment.assignment_generation().get(), 1);
    assert_eq!(classic_generation.get(), 7);
    assert_eq!(heartbeat.attempt().cycle(), cycle);
    assert_eq!(heartbeat.attempt().assignment_generation().get(), 1);
    assert_eq!(heartbeat.attempt().sequence().get(), 1);
    assert_eq!(heartbeat.due(), Deadline::from_tick(3));
    assert_eq!(
        heartbeat.liveness_deadline(),
        Deadline::from_tick(10_000_000_002)
    );
}

#[test]
fn exact_due_submits_one_assignment_fenced_heartbeat_with_its_own_deadline() {
    let (mut machine, _cycle, effect) = install(heartbeat_policy());
    let attempt = install_attempt(&effect);
    let early = machine
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt,
            now: Moment::from_tick(2),
        })
        .err()
        .unwrap_or_else(|| panic!("early cadence must reject"));
    assert_eq!(early.kind(), ClassicGroupErrorKind::DeadlineNotElapsed);

    let due = machine
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt,
            now: Moment::from_tick(3),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
    assert!(matches!(
        due.effects().next(),
        Some(ClassicGroupEffect::SubmitHeartbeat {
            attempt: emitted,
            member_id,
            classic_generation,
            deadline,
            ..
        }) if *emitted == attempt
            && member_id.get() == 2
            && classic_generation.get() == 7
            && *deadline == Deadline::from_tick(23)
    ));
}

#[test]
fn success_schedules_from_observation_and_honors_positive_throttle() {
    let (mut machine, _cycle, effect) = install(heartbeat_policy());
    let attempt = install_attempt(&effect);
    submit(&mut machine, attempt, 3);
    let success = machine
        .apply(ClassicGroupInput::HeartbeatSucceeded {
            attempt,
            now: Moment::from_tick(10),
            throttle_ticks: 30,
        })
        .unwrap_or_else(|error| panic!("successful heartbeat: {error}"));
    let Some(ClassicGroupEffect::ArmHeartbeat { schedule }) = success.effects().next() else {
        panic!("ArmHeartbeat effect expected");
    };

    assert_eq!(schedule.attempt().cycle(), attempt.cycle());
    assert_eq!(
        schedule.attempt().assignment_generation(),
        attempt.assignment_generation()
    );
    assert_eq!(schedule.attempt().sequence().get(), 2);
    assert_eq!(schedule.due(), Deadline::from_tick(40));
}

#[test]
fn early_deadline_and_stale_attempts_reject_without_mutation() {
    let (mut machine, _cycle, effect) = install(heartbeat_policy());
    let first = install_attempt(&effect);
    submit(&mut machine, first, 3);
    let early = machine
        .apply(ClassicGroupInput::HeartbeatDeadlineElapsed {
            attempt: first,
            now: Moment::from_tick(22),
        })
        .err()
        .unwrap_or_else(|| panic!("early attempt deadline must reject"));
    assert_eq!(early.kind(), ClassicGroupErrorKind::DeadlineNotElapsed);

    let next = arm_next(&mut machine, first);
    let stale = machine
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt: first,
            now: Moment::from_tick(100),
        })
        .err()
        .unwrap_or_else(|| panic!("stale sequence must reject"));
    assert_eq!(stale.kind(), ClassicGroupErrorKind::HeartbeatMismatch);
    assert_eq!(machine.phase(), ClassicGroupPhase::Stable);
    assert_eq!(next.sequence().get(), 2);
}

#[test]
fn failure_or_due_attempt_deadline_revokes_the_exact_live_assignment() {
    let (mut failed, _cycle, effect) = install(heartbeat_policy());
    let failed_attempt = install_attempt(&effect);
    submit(&mut failed, failed_attempt, 3);
    assert_revoke(
        &mut failed,
        ClassicGroupInput::HeartbeatFailed {
            attempt: failed_attempt,
        },
    );

    let (mut expired, _cycle, effect) = install(heartbeat_policy());
    let expired_attempt = install_attempt(&effect);
    submit(&mut expired, expired_attempt, 3);
    assert_revoke(
        &mut expired,
        ClassicGroupInput::HeartbeatDeadlineElapsed {
            attempt: expired_attempt,
            now: Moment::from_tick(23),
        },
    );

    let (mut late, _cycle, effect) = install(heartbeat_policy());
    let late_attempt = install_attempt(&effect);
    submit(&mut late, late_attempt, 3);
    assert_revoke(
        &mut late,
        ClassicGroupInput::HeartbeatSucceeded {
            attempt: late_attempt,
            now: Moment::from_tick(23),
            throttle_ticks: 0,
        },
    );
}

#[test]
fn assignment_loss_and_close_disarm_heartbeat_before_late_terminals() {
    let (mut lost, cycle, effect) = install(heartbeat_policy());
    let waiting = install_attempt(&effect);
    assert_revoke(&mut lost, ClassicGroupInput::AssignmentLost { cycle });
    assert_eq!(lost.phase(), ClassicGroupPhase::Lost);
    assert_late_rejects(&mut lost, waiting);

    let (mut closed, _cycle, effect) = install(heartbeat_policy());
    let inflight = install_attempt(&effect);
    submit(&mut closed, inflight, 3);
    assert_revoke(&mut closed, ClassicGroupInput::Close);
    assert_eq!(closed.phase(), ClassicGroupPhase::Closed);
    assert_late_rejects(&mut closed, inflight);
}

fn install(
    policy: ClassicHeartbeatPolicy,
) -> (ClassicGroupMachine, MembershipCycle, ClassicGroupEffect) {
    let mut machine = machine(policy);
    let cycle = begin(&mut machine);
    follow(&mut machine, cycle);
    let effect = machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(3),
            partitions: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("Sync success: {error}"))
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("Install effect expected"));
    (machine, cycle, effect)
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

fn submit(machine: &mut ClassicGroupMachine, attempt: ClassicHeartbeatAttempt, now: u64) {
    machine
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt,
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
}

fn arm_next(
    machine: &mut ClassicGroupMachine,
    attempt: ClassicHeartbeatAttempt,
) -> ClassicHeartbeatAttempt {
    let transition = machine
        .apply(ClassicGroupInput::HeartbeatSucceeded {
            attempt,
            now: Moment::from_tick(10),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("successful heartbeat: {error}"));
    let Some(ClassicGroupEffect::ArmHeartbeat { schedule }) = transition.effects().next() else {
        panic!("ArmHeartbeat effect expected");
    };
    schedule.attempt()
}

fn assert_revoke(machine: &mut ClassicGroupMachine, input: ClassicGroupInput) {
    let transition = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("membership loss: {error}"));
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Revoke {
            classic_generation,
            ..
        }) if classic_generation.get() == 7
    ));
    assert_eq!(machine.live_assignment(), None);
}

fn assert_late_rejects(machine: &mut ClassicGroupMachine, attempt: ClassicHeartbeatAttempt) {
    let error = machine
        .apply(ClassicGroupInput::HeartbeatSucceeded {
            attempt,
            now: Moment::from_tick(40),
            throttle_ticks: 0,
        })
        .err()
        .unwrap_or_else(|| panic!("late heartbeat must reject"));
    assert_eq!(error.kind(), ClassicGroupErrorKind::InvalidPhase);
}

fn install_attempt(effect: &ClassicGroupEffect) -> ClassicHeartbeatAttempt {
    let ClassicGroupEffect::Install { heartbeat, .. } = effect else {
        panic!("Install effect expected");
    };
    heartbeat.attempt()
}

fn heartbeat_policy() -> ClassicHeartbeatPolicy {
    ClassicHeartbeatPolicy::try_new(10, 20)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"))
}

fn machine(policy: ClassicHeartbeatPolicy) -> ClassicGroupMachine {
    ClassicGroupMachine::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group")),
        ClassicGroupTiming::try_new(10_000, 30_000)
            .unwrap_or_else(|error| panic!("valid group timing: {error}")),
        policy,
        super::ClassicRejoinPolicy::try_new(5, 50)
            .unwrap_or_else(|error| panic!("valid rejoin policy: {error:?}")),
    )
}
