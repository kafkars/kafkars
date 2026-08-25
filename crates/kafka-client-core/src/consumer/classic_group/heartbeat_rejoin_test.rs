//! Ordinary Heartbeat liveness-loss recovery evidence.

use crate::{Deadline, GroupId, MemberId, Moment};

use super::{
    ClassicCoordinatorRecovery, ClassicGeneration, ClassicGroupEffect, ClassicGroupInput,
    ClassicGroupMachine, ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatAttempt,
    ClassicHeartbeatPolicy, ClassicRejoinPolicy,
};

#[test]
fn ordinary_liveness_loss_revokes_then_arms_an_exact_retained_rejoin() {
    let (mut failed, failed_attempt) = stable(true);
    assert_rejoin(
        &mut failed,
        ClassicGroupInput::HeartbeatFailed {
            attempt: failed_attempt,
            now: Moment::from_tick(10),
        },
        failed_attempt,
        15,
    );

    let (mut expired, expired_attempt) = stable(true);
    assert_rejoin(
        &mut expired,
        ClassicGroupInput::HeartbeatDeadlineElapsed {
            attempt: expired_attempt,
            now: Moment::from_tick(23),
        },
        expired_attempt,
        28,
    );

    let (mut late, late_attempt) = stable(true);
    assert_rejoin(
        &mut late,
        ClassicGroupInput::HeartbeatSucceeded {
            attempt: late_attempt,
            now: Moment::from_tick(23),
            throttle_ticks: 0,
        },
        late_attempt,
        28,
    );

    let (mut unscheduled, unscheduled_attempt) = stable(false);
    assert_rejoin(
        &mut unscheduled,
        ClassicGroupInput::HeartbeatDue {
            attempt: unscheduled_attempt,
            now: Moment::from_tick(10_000_000_002),
        },
        unscheduled_attempt,
        10_000_000_007,
    );
}

fn assert_rejoin(
    machine: &mut ClassicGroupMachine,
    input: ClassicGroupInput,
    attempt: ClassicHeartbeatAttempt,
    due: u64,
) {
    let transition = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("membership loss: {error}"));
    let mut effects = transition.effects();
    assert!(matches!(
        effects.next(),
        Some(ClassicGroupEffect::Revoke {
            classic_generation,
            ..
        }) if classic_generation.get() == 7
    ));
    let Some(ClassicGroupEffect::ArmRejoin {
        schedule,
        coordinator: ClassicCoordinatorRecovery::Retain,
    }) = effects.next()
    else {
        panic!("retained rejoin expected");
    };
    assert_eq!(schedule.cycle(), attempt.cycle());
    assert_eq!(
        schedule.assignment_generation(),
        Some(attempt.assignment_generation())
    );
    assert_eq!(schedule.due(), Deadline::from_tick(due));
    assert!(effects.next().is_none());
    assert_eq!(machine.live_assignment(), None);
    assert_eq!(machine.phase(), ClassicGroupPhase::WaitingToRejoin);
    assert_eq!(machine.pending_rejoin(), Some(*schedule));
}

fn stable(inflight: bool) -> (ClassicGroupMachine, ClassicHeartbeatAttempt) {
    let mut machine = ClassicGroupMachine::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group")),
        ClassicGroupTiming::try_new(10_000, 30_000)
            .unwrap_or_else(|error| panic!("valid group timing: {error}")),
        ClassicHeartbeatPolicy::try_new(10, 20)
            .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}")),
        ClassicRejoinPolicy::try_new(5, 50)
            .unwrap_or_else(|error| panic!("valid rejoin policy: {error:?}")),
    );
    machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("valid Begin: {error}"));
    let cycle = machine.active_cycle().unwrap_or_else(|| panic!("cycle"));
    machine
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id: MemberId::try_from_raw(2).unwrap_or_else(|| panic!("member")),
            generation: ClassicGeneration::try_from_raw(7).unwrap_or_else(|| panic!("generation")),
        })
        .unwrap_or_else(|error| panic!("valid follower Join: {error}"));
    let transition = machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(3),
            partitions: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("Sync success: {error}"));
    let Some(ClassicGroupEffect::Install { heartbeat, .. }) = transition.effects().next() else {
        panic!("Install expected");
    };
    let attempt = heartbeat.attempt();
    if inflight {
        machine
            .apply(ClassicGroupInput::HeartbeatDue {
                attempt,
                now: Moment::from_tick(3),
            })
            .unwrap_or_else(|error| panic!("due Heartbeat: {error}"));
    }
    (machine, attempt)
}
