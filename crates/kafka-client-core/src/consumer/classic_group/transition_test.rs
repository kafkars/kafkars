//! Fenced classic Join and Sync lifecycle evidence.

use crate::{
    Deadline, GroupAssignmentPartition, GroupId, MemberId, Moment, PartitionIndex, TopicId,
};

use super::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupInput,
    ClassicGroupMachine, ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatPolicy,
    ClassicJoinMember, ClassicJoinMembers, ClassicProtocol, ClassicRejoinPolicy,
    ClassicSubscription, JoinedMemberSlot, MemberRank, MembershipCycle, TopicPartitionCount,
};

#[test]
fn start_emits_one_join_for_the_exact_cycle_without_activating_assignment() {
    let mut machine = machine();
    let transition = begin(&mut machine, 100);
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Join {
            cycle,
            protocol,
            deadline,
            ..
        }) if *cycle == MembershipCycle::initial()
            && *protocol == ClassicProtocol::Range
            && *deadline == Deadline::from_tick(100)
    ));
    assert_eq!(machine.live_assignment(), None);
}

#[test]
fn matching_join_success_emits_sync_without_activating_assignment() {
    let mut machine = machine();
    let cycle = begin_cycle(&mut machine, 100);
    let transition = machine
        .apply(follower(cycle, 2, 7))
        .unwrap_or_else(|error| panic!("valid follower Join: {error}"));
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Sync {
            generation,
            plan,
            deadline,
            ..
        }) if generation.get() == 7
            && plan.entries().is_empty()
            && *deadline == Deadline::from_tick(100)
    ));
    assert_eq!(machine.phase(), ClassicGroupPhase::Syncing);
    assert_eq!(machine.live_assignment(), None);
}

#[test]
fn leader_join_requires_core_owned_range_plan_before_sync() {
    let mut machine = machine();
    let cycle = begin_cycle(&mut machine, 100);
    let request = machine
        .apply(leader(cycle))
        .unwrap_or_else(|error| panic!("valid leader Join: {error}"));
    assert!(matches!(
        request.effects().next(),
        Some(ClassicGroupEffect::RequestPartitionCounts { topics, .. })
            if topics == &[topic(1), topic(2)]
    ));
    assert_eq!(machine.live_assignment(), None);

    let sync = machine
        .apply(ClassicGroupInput::PartitionCounts {
            cycle,
            now: Moment::from_tick(4),
            counts: vec![count(1, 5), count(2, 3)],
        })
        .unwrap_or_else(|error| panic!("valid partition counts: {error}"));
    assert!(matches!(
        sync.effects().next(),
        Some(ClassicGroupEffect::Sync { plan, .. })
            if plan.entries()[0].partitions().len() == 5
                && plan.entries()[1].partitions().len() == 2
                && plan.entries()[2].partitions().len() == 1
    ));
    assert_eq!(machine.live_assignment(), None);
}

#[test]
fn only_matching_sync_success_activates_one_assignment_generation() {
    let mut machine = machine();
    let cycle = begin_cycle(&mut machine, 100);
    machine
        .apply(follower(cycle, 5, 11))
        .unwrap_or_else(|error| panic!("valid follower Join: {error}"));
    let install = machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(6),
            partitions: vec![assigned(1, 3)],
        })
        .unwrap_or_else(|error| panic!("valid Sync: {error}"));

    assert!(matches!(
        install.effects().next(),
        Some(ClassicGroupEffect::Install {
            assignment,
            classic_generation,
            ..
        }) if assignment.assignment_generation().get() == 1
            && classic_generation.get() == 11
    ));
    assert_eq!(
        machine.live_generation().map(ClassicGeneration::get),
        Some(11)
    );
    assert_eq!(
        machine
            .live_assignment()
            .map(|assignment| assignment.assignment_generation().get()),
        Some(1)
    );
}

#[test]
fn stale_and_out_of_phase_join_or_sync_facts_reject_without_mutation() {
    let mut machine = machine();
    let cycle = begin_cycle(&mut machine, 100);
    let stale = cycle
        .checked_next()
        .unwrap_or_else(|| panic!("second cycle"));
    let error = machine
        .apply(follower(stale, 1, 1))
        .err()
        .unwrap_or_else(|| panic!("stale Join must reject"));
    assert_eq!(error.kind(), ClassicGroupErrorKind::CycleMismatch);
    assert_eq!(machine.phase(), ClassicGroupPhase::Joining);
    assert_eq!(machine.active_cycle(), Some(cycle));

    machine
        .apply(follower(cycle, 1, 1))
        .unwrap_or_else(|error| panic!("valid follower Join: {error}"));
    let error = machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle: stale,
            now: Moment::from_tick(5),
            partitions: Vec::new(),
        })
        .err()
        .unwrap_or_else(|| panic!("stale Sync must reject"));
    assert_eq!(error.kind(), ClassicGroupErrorKind::CycleMismatch);
    assert_eq!(machine.phase(), ClassicGroupPhase::Syncing);
    assert_eq!(machine.live_assignment(), None);
}

#[test]
fn second_join_cycle_cannot_reuse_the_first_cycle_or_assignment_generation() {
    let mut machine = machine();
    install_empty(&mut machine, 1, 3);
    let first_cycle = machine
        .active_cycle()
        .unwrap_or_else(|| panic!("stable cycle"));
    let loss = machine
        .apply(ClassicGroupInput::AssignmentLost { cycle: first_cycle })
        .unwrap_or_else(|error| panic!("valid assignment loss: {error}"));
    assert!(matches!(
        loss.effects().next(),
        Some(ClassicGroupEffect::Revoke {
            classic_generation,
            ..
        }) if classic_generation.get() == 3
    ));
    let transition = begin(&mut machine, 200);
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Join { cycle, .. }) if cycle.get() == 2
    ));
    let cycle = machine
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle"));
    machine
        .apply(follower(cycle, 1, 4))
        .unwrap_or_else(|error| panic!("valid follower Join: {error}"));
    machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(10),
            partitions: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("valid second Sync: {error}"));
    assert_eq!(
        machine
            .live_assignment()
            .map(|assignment| assignment.assignment_generation().get()),
        Some(2)
    );
}

fn install_empty(machine: &mut ClassicGroupMachine, member_id: u64, generation: i32) {
    let cycle = begin_cycle(machine, 100);
    machine
        .apply(follower(cycle, member_id, generation))
        .unwrap_or_else(|error| panic!("valid follower Join: {error}"));
    machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(5),
            partitions: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("valid Sync: {error}"));
}

fn leader(cycle: MembershipCycle) -> ClassicGroupInput {
    ClassicGroupInput::JoinLeader {
        cycle,
        now: Moment::from_tick(3),
        member_id: member(1),
        local_slot: slot(1),
        generation: generation(9),
        members: joined(&[(1, &[1, 2]), (2, &[1]), (3, &[2])]),
    }
}

fn joined(specification: &[(u32, &[u64])]) -> ClassicJoinMembers {
    let members = specification
        .iter()
        .map(|(raw, topics)| {
            let subscription =
                ClassicSubscription::try_new(topics.iter().copied().map(topic).collect())
                    .unwrap_or_else(|error| panic!("valid topics: {error:?}"));
            ClassicJoinMember::new(
                slot(*raw),
                member(u64::from(*raw)),
                rank(*raw),
                subscription,
            )
        })
        .collect();
    ClassicJoinMembers::try_new(members).unwrap_or_else(|error| panic!("valid members: {error:?}"))
}

fn follower(cycle: MembershipCycle, member_id: u64, raw_generation: i32) -> ClassicGroupInput {
    ClassicGroupInput::JoinFollower {
        cycle,
        now: Moment::from_tick(3),
        member_id: member(member_id),
        generation: generation(raw_generation),
    }
}

fn begin_cycle(machine: &mut ClassicGroupMachine, deadline: u64) -> MembershipCycle {
    begin(machine, deadline);
    machine
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle"))
}

fn begin(machine: &mut ClassicGroupMachine, deadline: u64) -> super::ClassicGroupTransition {
    machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(deadline),
        })
        .unwrap_or_else(|error| panic!("valid begin: {error}"))
}

fn machine() -> ClassicGroupMachine {
    ClassicGroupMachine::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group")),
        ClassicGroupTiming::try_new(10_000, 30_000)
            .unwrap_or_else(|error| panic!("valid timing: {error}")),
        ClassicHeartbeatPolicy::try_new(10, 20)
            .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}")),
        ClassicRejoinPolicy::try_new(5, 50).unwrap_or_else(|_| panic!("valid rejoin")),
    )
}

fn assigned(topic_id: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(topic(topic_id), PartitionIndex::from_raw(partition))
}

fn count(topic_id: u64, partitions: u32) -> TopicPartitionCount {
    TopicPartitionCount::new(topic(topic_id), partitions)
}

fn slot(value: u32) -> JoinedMemberSlot {
    JoinedMemberSlot::try_from_raw(value).unwrap_or_else(|| panic!("nonzero slot"))
}

fn rank(value: u32) -> MemberRank {
    MemberRank::try_from_raw(value).unwrap_or_else(|| panic!("nonzero rank"))
}

fn member(value: u64) -> MemberId {
    MemberId::try_from_raw(value).unwrap_or_else(|| panic!("nonzero member"))
}

fn generation(value: i32) -> ClassicGeneration {
    ClassicGeneration::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative generation"))
}

const fn topic(value: u64) -> TopicId {
    TopicId::from_raw(value)
}
