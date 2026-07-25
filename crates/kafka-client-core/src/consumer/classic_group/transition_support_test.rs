//! Pure classic transition validation and topic-union evidence.

use crate::{Deadline, GroupId, MemberId, Moment, TopicId};

use super::{
    ClassicGroupInput, ClassicGroupMachine, ClassicGroupPhase, ClassicHeartbeatPolicy,
    ClassicJoinMember, ClassicJoinMembers, ClassicSubscription, JoinedMemberSlot, MemberRank,
    MembershipCycle,
    transition_support::{collect_group_topics, local_member_is_present, validate_active},
};

#[test]
fn group_topic_union_is_sorted_unique_and_member_identity_is_exact() {
    let members = joined(&[(1, &[1, 3]), (2, &[2, 3])]);
    assert_eq!(
        collect_group_topics(&members),
        Ok(vec![topic(1), topic(2), topic(3)])
    );
    assert!(local_member_is_present(&members, slot(2), member(2)));
    assert!(!local_member_is_present(&members, slot(1), member(2)));
}

#[test]
fn active_validation_checks_phase_cycle_and_original_deadline() {
    let mut machine = machine();
    begin(&mut machine, 10);
    let cycle = MembershipCycle::initial();
    assert_eq!(
        validate_active(
            &machine,
            ClassicGroupPhase::Joining,
            cycle,
            Moment::from_tick(9),
        ),
        Ok(Deadline::from_tick(10))
    );
    assert!(
        validate_active(
            &machine,
            ClassicGroupPhase::Syncing,
            cycle,
            Moment::from_tick(9),
        )
        .is_err()
    );
    assert!(
        validate_active(
            &machine,
            ClassicGroupPhase::Joining,
            cycle,
            Moment::from_tick(10),
        )
        .is_err()
    );
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

fn begin(machine: &mut ClassicGroupMachine, deadline: u64) {
    machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(deadline),
        })
        .unwrap_or_else(|error| panic!("valid begin: {error}"));
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
