//! Deterministic KIP-394 same-cycle Join replacement evidence.

use crate::{Deadline, GroupId, MemberId, Moment};

use super::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupInput,
    ClassicGroupMachine, ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatPolicy,
    ClassicRejoinPolicy, MembershipCycle,
};

#[test]
fn replacement_retains_cycle_identity_and_original_deadline_once() {
    let mut machine = machine();
    let cycle = begin(&mut machine, 100);
    let replacement = require_member(&mut machine, cycle, 2, Some(member(7)))
        .unwrap_or_else(|error| panic!("replacement: {error}"));
    assert!(matches!(
        replacement.effects().next(),
        Some(ClassicGroupEffect::Join {
            cycle: effect_cycle,
            member_id: Some(effect_member),
            deadline,
            ..
        }) if *effect_cycle == cycle
            && *effect_member == member(7)
            && *deadline == Deadline::from_tick(100)
    ));
    assert_eq!(replacement.effects().count(), 1);
    assert_eq!(machine.phase(), ClassicGroupPhase::Joining);
    let Err(duplicate) = require_member(&mut machine, cycle, 3, Some(member(7))) else {
        panic!("duplicate replacement");
    };
    assert_eq!(
        duplicate.kind(),
        ClassicGroupErrorKind::DuplicateAssignedMemberId
    );
}

#[test]
fn malformed_stale_and_expired_replacements_are_rejected() {
    let mut machine = machine();
    let cycle = begin(&mut machine, 20);
    let Err(missing) = require_member(&mut machine, cycle, 2, None) else {
        panic!("missing identity");
    };
    assert_eq!(
        missing.kind(),
        ClassicGroupErrorKind::MissingAssignedMemberId
    );
    let stale_cycle = cycle.checked_next().unwrap_or_else(|| panic!("next cycle"));
    let Err(stale) = require_member(&mut machine, stale_cycle, 3, Some(member(7))) else {
        panic!("stale cycle");
    };
    assert_eq!(stale.kind(), ClassicGroupErrorKind::CycleMismatch);
    let Err(expired) = require_member(&mut machine, cycle, 20, Some(member(7))) else {
        panic!("expired replacement");
    };
    assert_eq!(expired.kind(), ClassicGroupErrorKind::DeadlineElapsed);
}

#[test]
fn join_success_must_match_the_assigned_identity() {
    let mut machine = machine();
    let cycle = begin(&mut machine, 100);
    require_member(&mut machine, cycle, 2, Some(member(7)))
        .unwrap_or_else(|error| panic!("replacement: {error}"));
    let Err(mismatch) = machine.apply(follower(cycle, 8)) else {
        panic!("mismatched identity");
    };
    assert_eq!(
        mismatch.kind(),
        ClassicGroupErrorKind::AssignedMemberIdMismatch
    );
    assert!(machine.apply(follower(cycle, 7)).is_ok());
}

fn require_member(
    machine: &mut ClassicGroupMachine,
    cycle: MembershipCycle,
    now: u64,
    assigned_member_id: Option<MemberId>,
) -> Result<super::ClassicGroupTransition, super::ClassicGroupApplyError> {
    machine.apply(ClassicGroupInput::JoinMemberIdRequired {
        cycle,
        now: Moment::from_tick(now),
        assigned_member_id,
    })
}

fn follower(cycle: MembershipCycle, raw_member: u64) -> ClassicGroupInput {
    ClassicGroupInput::JoinFollower {
        cycle,
        now: Moment::from_tick(3),
        member_id: member(raw_member),
        generation: ClassicGeneration::try_from_raw(11).unwrap_or_else(|| panic!("generation")),
    }
}

fn begin(machine: &mut ClassicGroupMachine, deadline: u64) -> MembershipCycle {
    let transition = machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(deadline),
        })
        .unwrap_or_else(|error| panic!("begin: {error}"));
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Join {
            member_id: None,
            ..
        })
    ));
    machine.active_cycle().unwrap_or_else(|| panic!("cycle"))
}

fn machine() -> ClassicGroupMachine {
    ClassicGroupMachine::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        ClassicGroupTiming::try_new(10_000, 30_000)
            .unwrap_or_else(|error| panic!("timing: {error}")),
        ClassicHeartbeatPolicy::try_new(10, 20)
            .unwrap_or_else(|error| panic!("heartbeat: {error}")),
        ClassicRejoinPolicy::try_new(5, 50).unwrap_or_else(|error| panic!("rejoin: {error:?}")),
    )
}

fn member(raw: u64) -> MemberId {
    MemberId::try_from_raw(raw).unwrap_or_else(|| panic!("member"))
}
