//! Cooperative reconciliation delta, fencing, and multi-round lifecycle tests.

#![expect(
    clippy::cast_possible_truncation,
    reason = "fixture indices are bounded far below the domain integer limit"
)]

use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, MemberId, Moment,
    PartitionIndex, TopicId,
};

use super::{
    ClassicBrokerError, ClassicGeneration, ClassicGroupEffect, ClassicGroupInput,
    ClassicGroupMachine, ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatPolicy,
    ClassicJoinMember, ClassicJoinMembers, ClassicProtocol, ClassicRejoinPolicy,
    ClassicSubscription, JoinedMemberSlot, MemberRank, MembershipCycle, TopicPartitionCount,
};

#[test]
fn follower_reconciliation_emits_exact_delta_and_requires_removal_followup() {
    let mut machine = cooperative_machine();
    let (first, heartbeat) = install(&mut machine, &[0, 1, 3]);
    let second = begin_retained_cycle(&mut machine, first, heartbeat);
    follow(&mut machine, second, 8, 10);

    let transition = sync(&mut machine, second, &[0, 2, 3], 11);
    let Some(ClassicGroupEffect::Reconcile { reconciliation }) = transition.effects().next() else {
        panic!("Reconcile expected");
    };
    assert_eq!(reconciliation.previous_cycle(), first);
    assert_eq!(reconciliation.previous_classic_generation().get(), 7);
    assert_eq!(
        reconciliation
            .previous_assignment()
            .assignment_generation()
            .get(),
        1
    );
    assert_eq!(reconciliation.replacement_cycle(), second);
    assert_eq!(reconciliation.replacement_classic_generation().get(), 8);
    assert_eq!(
        reconciliation
            .replacement_assignment()
            .assignment_generation()
            .get(),
        2
    );
    assert_eq!(raw(reconciliation.delta().retained()), vec![0, 3]);
    assert_eq!(raw(reconciliation.delta().removed()), vec![1]);
    assert_eq!(raw(reconciliation.delta().added()), vec![2]);
    assert!(reconciliation.requires_followup());
    assert_eq!(machine.phase(), ClassicGroupPhase::Reconciling);

    let heartbeat = reconciliation.heartbeat();
    assert!(matches!(
        machine
            .apply(ClassicGroupInput::HeartbeatDue {
                attempt: heartbeat.attempt(),
                now: Moment::from_tick(11),
            })
            .unwrap_or_else(|error| panic!("live reconciliation heartbeat: {error}"))
            .effects()
            .next(),
        Some(ClassicGroupEffect::SubmitHeartbeat { .. })
    ));
    let stale = machine
        .apply(ClassicGroupInput::ReconciliationApplied {
            cycle: second,
            assignment_generation: AssignmentGeneration::initial(),
            now: Moment::from_tick(12),
        })
        .err()
        .unwrap_or_else(|| panic!("stale reconciliation must reject"));
    assert_eq!(
        stale.kind(),
        super::ClassicGroupErrorKind::HeartbeatMismatch
    );
    assert_eq!(machine.phase(), ClassicGroupPhase::Reconciling);

    let followup = apply_reconciliation(&mut machine, second, 2, 12);
    let Some(ClassicGroupEffect::Join {
        cycle: third,
        member_id: Some(member_id),
        deadline,
        ..
    }) = followup.effects().next()
    else {
        panic!("fresh retained Join expected");
    };
    assert_eq!(member_id.get(), 1);
    assert_eq!(*deadline, Deadline::from_tick(62));
    let third = *third;
    follow(&mut machine, third, 9, 13);
    let settled = sync(&mut machine, third, &[0, 2, 3], 14);
    let Some(ClassicGroupEffect::Reconcile { reconciliation }) = settled.effects().next() else {
        panic!("second Reconcile expected");
    };
    assert!(!reconciliation.requires_followup());
    assert_eq!(raw(reconciliation.delta().retained()), vec![0, 2, 3]);
    assert!(reconciliation.delta().removed().is_empty());
    assert!(reconciliation.delta().added().is_empty());
    apply_reconciliation(&mut machine, third, 3, 15);
    assert_eq!(machine.phase(), ClassicGroupPhase::Stable);
}

#[test]
fn leader_marks_followup_when_a_foreign_transfer_was_withheld() {
    let mut machine = cooperative_machine();
    let (first, heartbeat) = install(&mut machine, &[0, 1]);
    let second = begin_retained_cycle(&mut machine, first, heartbeat);
    let members = joined_members(&[(&[0, 1][..], 1), (&[2, 3, 4, 5][..], 1), (&[][..], 1)]);
    machine
        .apply(ClassicGroupInput::JoinLeader {
            cycle: second,
            now: Moment::from_tick(10),
            member_id: member(1),
            local_slot: slot(1),
            generation: generation(8),
            members,
        })
        .unwrap_or_else(|error| panic!("leader Join: {error}"));
    let planned = machine
        .apply(ClassicGroupInput::PartitionCounts {
            cycle: second,
            now: Moment::from_tick(11),
            counts: vec![TopicPartitionCount::new(topic(), 6)],
        })
        .unwrap_or_else(|error| panic!("partition counts: {error}"));
    assert!(matches!(
        planned.effects().next(),
        Some(ClassicGroupEffect::Sync { plan, .. }) if plan.requires_followup()
    ));
    let reconciled = sync(&mut machine, second, &[0, 1], 12);
    let Some(ClassicGroupEffect::Reconcile { reconciliation }) = reconciled.effects().next() else {
        panic!("leader Reconcile expected");
    };
    assert!(reconciliation.delta().removed().is_empty());
    assert!(reconciliation.requires_followup());
}

fn install(
    machine: &mut ClassicGroupMachine,
    partitions: &[u32],
) -> (MembershipCycle, super::ClassicHeartbeatSchedule) {
    let cycle = begin(machine);
    follow(machine, cycle, 7, 2);
    let transition = sync(machine, cycle, partitions, 3);
    let Some(ClassicGroupEffect::Install { heartbeat, .. }) = transition.effects().next() else {
        panic!("Install expected");
    };
    (cycle, *heartbeat)
}

fn begin_retained_cycle(
    machine: &mut ClassicGroupMachine,
    cycle: MembershipCycle,
    heartbeat: super::ClassicHeartbeatSchedule,
) -> MembershipCycle {
    let attempt = heartbeat.attempt();
    machine
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt,
            now: Moment::from_tick(heartbeat.due().tick()),
        })
        .unwrap_or_else(|error| panic!("heartbeat due: {error}"));
    let rejected = machine
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt,
            now: Moment::from_tick(4),
            error: broker_error(27),
        })
        .unwrap_or_else(|error| panic!("rebalance heartbeat: {error}"));
    let Some(ClassicGroupEffect::ArmRejoin { schedule, .. }) = rejected.effects().next() else {
        panic!("ArmRejoin expected");
    };
    assert_eq!(schedule.cycle(), cycle);
    let schedule = *schedule;
    machine
        .apply(ClassicGroupInput::RejoinDue {
            schedule,
            now: Moment::from_tick(schedule.due().tick()),
        })
        .unwrap_or_else(|error| panic!("rejoin due: {error}"));
    machine
        .active_cycle()
        .unwrap_or_else(|| panic!("new cycle"))
}

fn apply_reconciliation(
    machine: &mut ClassicGroupMachine,
    cycle: MembershipCycle,
    generation: u64,
    now: u64,
) -> super::ClassicGroupTransition {
    machine
        .apply(ClassicGroupInput::ReconciliationApplied {
            cycle,
            assignment_generation: AssignmentGeneration::try_from_raw(generation)
                .unwrap_or_else(|| panic!("nonzero assignment generation")),
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("reconciliation applied: {error}"))
}

fn begin(machine: &mut ClassicGroupMachine) -> MembershipCycle {
    machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("Begin: {error}"));
    machine.active_cycle().unwrap_or_else(|| panic!("cycle"))
}

fn follow(machine: &mut ClassicGroupMachine, cycle: MembershipCycle, value: i32, now: u64) {
    machine
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(now),
            member_id: member(1),
            generation: generation(value),
        })
        .unwrap_or_else(|error| panic!("follower Join: {error}"));
}

fn sync(
    machine: &mut ClassicGroupMachine,
    cycle: MembershipCycle,
    partitions: &[u32],
    now: u64,
) -> super::ClassicGroupTransition {
    machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(now),
            partitions: partitions.iter().copied().map(partition).collect(),
        })
        .unwrap_or_else(|error| panic!("Sync: {error}"))
}

fn joined_members(specifications: &[(&[u32], i32)]) -> ClassicJoinMembers {
    let members = specifications
        .iter()
        .enumerate()
        .map(|(index, (owned, generation_value))| {
            let raw = (index + 1) as u32;
            let subscription = ClassicSubscription::try_new_with_owned(
                vec![topic()],
                owned.iter().copied().map(partition).collect(),
                Some(generation(*generation_value)),
            )
            .unwrap_or_else(|error| panic!("subscription: {error:?}"));
            ClassicJoinMember::new(slot(raw), member(u64::from(raw)), rank(raw), subscription)
        })
        .collect();
    ClassicJoinMembers::try_new(members).unwrap_or_else(|error| panic!("members: {error:?}"))
}

fn cooperative_machine() -> ClassicGroupMachine {
    ClassicGroupMachine::new_with_protocol(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        ClassicProtocol::CooperativeSticky,
        ClassicGroupTiming::try_new(10_000, 30_000)
            .unwrap_or_else(|error| panic!("timing: {error}")),
        ClassicHeartbeatPolicy::try_new(10, 20)
            .unwrap_or_else(|error| panic!("heartbeat: {error}")),
        ClassicRejoinPolicy::try_new(5, 50).unwrap_or_else(|error| panic!("rejoin: {error:?}")),
    )
}

fn raw(partitions: &[GroupAssignmentPartition]) -> Vec<u32> {
    partitions
        .iter()
        .map(|partition| partition.partition().get())
        .collect()
}

fn partition(value: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(topic(), PartitionIndex::from_raw(value))
}

fn topic() -> TopicId {
    TopicId::from_raw(1)
}
fn member(value: u64) -> MemberId {
    MemberId::try_from_raw(value).unwrap_or_else(|| panic!("member"))
}
fn slot(value: u32) -> JoinedMemberSlot {
    JoinedMemberSlot::try_from_raw(value).unwrap_or_else(|| panic!("slot"))
}
fn rank(value: u32) -> MemberRank {
    MemberRank::try_from_raw(value).unwrap_or_else(|| panic!("rank"))
}
fn generation(value: i32) -> ClassicGeneration {
    ClassicGeneration::try_from_raw(value).unwrap_or_else(|| panic!("generation"))
}
fn broker_error(code: i16) -> ClassicBrokerError {
    ClassicBrokerError::try_from_code(code).unwrap_or_else(|| panic!("broker error"))
}
