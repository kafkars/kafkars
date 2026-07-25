//! Classic membership loss and close ordering evidence.

use crate::{Deadline, GroupId, MemberId, Moment};

use super::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupInput, ClassicGroupMachine,
    ClassicGroupPhase, ClassicHeartbeatPolicy, MembershipCycle,
};

#[test]
fn failed_join_or_sync_never_installs_a_live_assignment() {
    let mut joining = machine();
    let cycle = begin(&mut joining);
    joining
        .apply(ClassicGroupInput::JoinFailed { cycle })
        .unwrap_or_else(|error| panic!("valid failure: {error}"));
    assert_eq!(joining.phase(), ClassicGroupPhase::Lost);
    assert_eq!(joining.live_assignment(), None);

    let mut syncing = machine();
    let cycle = begin(&mut syncing);
    follow(&mut syncing, cycle, 3);
    syncing
        .apply(ClassicGroupInput::SyncFailed { cycle })
        .unwrap_or_else(|error| panic!("valid Sync failure: {error}"));
    assert_eq!(syncing.phase(), ClassicGroupPhase::Lost);
    assert_eq!(syncing.live_assignment(), None);
}

#[test]
fn deadline_fires_only_at_the_original_absolute_moment() {
    let mut machine = machine();
    let cycle = begin(&mut machine);
    let early = machine
        .apply(ClassicGroupInput::DeadlineElapsed {
            cycle,
            now: Moment::from_tick(99),
        })
        .err()
        .unwrap_or_else(|| panic!("early deadline must reject"));
    assert_eq!(
        early.kind(),
        super::ClassicGroupErrorKind::DeadlineNotElapsed
    );
    machine
        .apply(ClassicGroupInput::DeadlineElapsed {
            cycle,
            now: Moment::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("elapsed deadline: {error}"));
    assert_eq!(machine.phase(), ClassicGroupPhase::Lost);
}

#[test]
fn delayed_join_failure_cannot_kill_the_same_cycles_sync_stage() {
    let mut machine = machine();
    let cycle = begin(&mut machine);
    follow(&mut machine, cycle, 4);
    let error = machine
        .apply(ClassicGroupInput::JoinFailed { cycle })
        .err()
        .unwrap_or_else(|| panic!("late Join failure must reject"));

    assert_eq!(error.kind(), super::ClassicGroupErrorKind::InvalidPhase);
    assert_eq!(machine.phase(), ClassicGroupPhase::Syncing);
}

#[test]
fn stable_close_revokes_exact_assignment_and_kafka_generation() {
    let mut machine = machine();
    let cycle = begin(&mut machine);
    follow(&mut machine, cycle, 9);
    machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(4),
            partitions: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("valid Sync: {error}"));
    let close = machine
        .apply(ClassicGroupInput::Close)
        .unwrap_or_else(|error| panic!("valid close: {error}"));

    assert!(matches!(
        close.effects().next(),
        Some(ClassicGroupEffect::Revoke {
            classic_generation,
            ..
        }) if classic_generation.get() == 9
    ));
    assert_eq!(machine.phase(), ClassicGroupPhase::Closed);
    assert_eq!(machine.live_assignment(), None);
    assert_eq!(machine.live_generation(), None);
}

fn begin(machine: &mut ClassicGroupMachine) -> MembershipCycle {
    machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("valid begin: {error}"));
    machine
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle"))
}

fn follow(machine: &mut ClassicGroupMachine, cycle: MembershipCycle, generation: i32) {
    machine
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id: MemberId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero member")),
            generation: ClassicGeneration::try_from_raw(generation)
                .unwrap_or_else(|| panic!("nonnegative generation")),
        })
        .unwrap_or_else(|error| panic!("valid follower Join: {error}"));
}

fn machine() -> ClassicGroupMachine {
    ClassicGroupMachine::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group")),
        super::ClassicGroupTiming::try_new(10_000, 30_000)
            .unwrap_or_else(|error| panic!("valid timing: {error}")),
        ClassicHeartbeatPolicy::try_new(10, 20)
            .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}")),
    )
}
