//! Leader-computed local Range assignment activation fencing evidence.

use crate::{
    Deadline, GroupAssignmentPartition, GroupId, MemberId, Moment, PartitionIndex, TopicId,
};

use super::{
    ClassicGeneration, ClassicGroupErrorKind, ClassicGroupInput, ClassicGroupMachine,
    ClassicGroupPhase, ClassicHeartbeatPolicy, ClassicJoinMember, ClassicJoinMembers,
    ClassicSubscription, JoinedMemberSlot, MemberRank, MembershipCycle, TopicPartitionCount,
};

#[test]
fn leader_sync_rejects_a_local_assignment_different_from_the_core_plan() {
    let mut machine = machine();
    let cycle = begin(&mut machine);
    machine
        .apply(leader(cycle))
        .unwrap_or_else(|error| panic!("valid leader Join: {error}"));
    machine
        .apply(ClassicGroupInput::PartitionCounts {
            cycle,
            now: Moment::from_tick(3),
            counts: vec![TopicPartitionCount::new(topic(1), 3)],
        })
        .unwrap_or_else(|error| panic!("valid counts: {error}"));

    let error = machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(4),
            partitions: vec![assigned(1, 2)],
        })
        .err()
        .unwrap_or_else(|| panic!("mismatched leader assignment must reject"));
    assert_eq!(
        error.kind(),
        ClassicGroupErrorKind::LeaderAssignmentMismatch
    );
    assert_eq!(machine.phase(), ClassicGroupPhase::Syncing);
    assert_eq!(machine.live_assignment(), None);

    machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(5),
            partitions: vec![assigned(1, 0), assigned(1, 1)],
        })
        .unwrap_or_else(|error| panic!("matching leader assignment: {error}"));
    assert_eq!(machine.phase(), ClassicGroupPhase::Stable);
}

#[test]
fn partition_count_failure_is_stage_fenced_from_join_and_sync() {
    let mut machine = machine();
    let cycle = begin(&mut machine);
    machine
        .apply(leader(cycle))
        .unwrap_or_else(|error| panic!("valid leader Join: {error}"));
    let delayed_join = machine
        .apply(ClassicGroupInput::JoinFailed { cycle })
        .err()
        .unwrap_or_else(|| panic!("delayed Join failure must reject"));
    assert_eq!(delayed_join.kind(), ClassicGroupErrorKind::InvalidPhase);
    assert_eq!(machine.phase(), ClassicGroupPhase::AwaitingPartitionCounts);
    machine
        .apply(ClassicGroupInput::PartitionCountsFailed { cycle })
        .unwrap_or_else(|error| panic!("valid count failure: {error}"));
    assert_eq!(machine.phase(), ClassicGroupPhase::Lost);
}

fn leader(cycle: MembershipCycle) -> ClassicGroupInput {
    let subscription = ClassicSubscription::try_new(vec![topic(1)])
        .unwrap_or_else(|error| panic!("valid subscription: {error:?}"));
    let local = ClassicJoinMember::new(slot(1), member(1), rank(1), subscription);
    let other_subscription = ClassicSubscription::try_new(vec![topic(1)])
        .unwrap_or_else(|error| panic!("valid subscription: {error:?}"));
    let other = ClassicJoinMember::new(slot(2), member(2), rank(2), other_subscription);
    let members = ClassicJoinMembers::try_new(vec![local, other])
        .unwrap_or_else(|error| panic!("valid members: {error:?}"));
    ClassicGroupInput::JoinLeader {
        cycle,
        now: Moment::from_tick(2),
        member_id: member(1),
        local_slot: slot(1),
        generation: ClassicGeneration::try_from_raw(7)
            .unwrap_or_else(|| panic!("nonnegative generation")),
        members,
    }
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

fn machine() -> ClassicGroupMachine {
    ClassicGroupMachine::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group")),
        super::ClassicGroupTiming::try_new(10_000, 30_000)
            .unwrap_or_else(|error| panic!("valid timing: {error}")),
        ClassicHeartbeatPolicy::try_new(10, 20)
            .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}")),
        super::ClassicRejoinPolicy::try_new(5, 50)
            .unwrap_or_else(|error| panic!("valid rejoin policy: {error:?}")),
    )
}

fn assigned(topic_id: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(topic(topic_id), PartitionIndex::from_raw(partition))
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

const fn topic(value: u64) -> TopicId {
    TopicId::from_raw(value)
}
