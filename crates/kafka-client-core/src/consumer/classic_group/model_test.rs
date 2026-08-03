//! Bounded normalized classic-group model evidence.

use crate::{GroupAssignmentPartition, MemberId, PartitionIndex, TopicId};

use super::model::{MAX_CLASSIC_GROUP_MEMBERS, MAX_CLASSIC_TOPICS_PER_MEMBER};
use super::{
    ClassicGroupPhase, ClassicJoinMember, ClassicJoinMembers, ClassicJoinMembersError,
    ClassicProtocol, ClassicSubscription, ClassicSubscriptionError, JoinedMemberSlot, MemberRank,
    TopicPartitionCount,
};

#[test]
fn subscription_accepts_only_bounded_ordered_unique_topic_facts() {
    let subscription = ClassicSubscription::try_new(vec![topic(1), topic(3)])
        .unwrap_or_else(|error| panic!("valid subscription: {error:?}"));
    assert_eq!(subscription.topics(), [topic(1), topic(3)]);
    assert_eq!(
        ClassicSubscription::try_new(vec![topic(1), topic(1)]),
        Err(ClassicSubscriptionError::DuplicateTopic(topic(1)))
    );
    assert_eq!(
        ClassicSubscription::try_new(vec![topic(2), topic(1)]),
        Err(ClassicSubscriptionError::OutOfOrder)
    );
    assert_eq!(
        ClassicSubscription::try_new(vec![topic(1); MAX_CLASSIC_TOPICS_PER_MEMBER + 1]),
        Err(ClassicSubscriptionError::TooManyTopics)
    );
}

#[test]
fn subscription_retains_prior_ownership_from_a_dropped_topic() {
    let dropped = GroupAssignmentPartition::new(topic(1), PartitionIndex::from_raw(3));
    let subscription = ClassicSubscription::try_new_with_owned(vec![topic(2)], vec![dropped], None)
        .unwrap_or_else(|error| panic!("dropped-topic ownership: {error:?}"));

    assert_eq!(subscription.topics(), [topic(2)]);
    assert_eq!(subscription.owned_partitions(), [dropped]);
}

#[test]
fn join_members_are_bounded_unique_and_ranked_by_kafka_identity() {
    let members = ClassicJoinMembers::try_new(vec![member_fact(1), member_fact(2)])
        .unwrap_or_else(|error| panic!("valid member facts: {error:?}"));
    assert_eq!(members.members().len(), 2);
    assert_eq!(
        ClassicJoinMembers::try_new(Vec::new()),
        Err(ClassicJoinMembersError::Empty)
    );

    let duplicated = vec![
        member_fact(1),
        ClassicJoinMember::new(slot(1), member(2), rank(2), subscription(2)),
    ];
    assert_eq!(
        ClassicJoinMembers::try_new(duplicated),
        Err(ClassicJoinMembersError::DuplicateSlot(slot(1)))
    );

    let oversized = (1..=MAX_CLASSIC_GROUP_MEMBERS + 1)
        .map(|value| {
            member_fact(u32::try_from(value).unwrap_or_else(|_| panic!("small member bound")))
        })
        .collect();
    assert_eq!(
        ClassicJoinMembers::try_new(oversized),
        Err(ClassicJoinMembersError::TooManyMembers)
    );
}

#[test]
fn model_is_scalar_and_phase_and_partition_counts_are_exact() {
    assert_ne!(ClassicGroupPhase::Joining, ClassicGroupPhase::Stable);
    assert_eq!(ClassicProtocol::Range, ClassicProtocol::Range);
    let count = TopicPartitionCount::new(topic(9), 0);
    assert_eq!(count.topic_id(), topic(9));
    assert_eq!(count.count(), 0);
}

fn member_fact(value: u32) -> ClassicJoinMember {
    ClassicJoinMember::new(
        slot(value),
        member(u64::from(value)),
        rank(value),
        subscription(u64::from(value)),
    )
}

fn subscription(value: u64) -> ClassicSubscription {
    ClassicSubscription::try_new(vec![topic(value)])
        .unwrap_or_else(|error| panic!("valid subscription: {error:?}"))
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
