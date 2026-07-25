//! Golden evidence for bounded deterministic classic Range assignment.

use crate::{MemberId, TopicId};

use super::{
    ClassicAssignmentError, ClassicAssignmentPlan, ClassicJoinMember, ClassicJoinMembers,
    ClassicMemberAssignment, ClassicSubscription, JoinedMemberSlot, MemberRank,
    TopicPartitionCount,
};

#[test]
fn range_splits_eight_partitions_across_three_ranked_members() {
    let members = joined(&[(1, &[7]), (2, &[7]), (3, &[7])]);
    let plan = plan(&members, &[(7, 8)]);
    assert_partitions(&plan.entries()[0], &[(7, 0), (7, 1), (7, 2)]);
    assert_partitions(&plan.entries()[1], &[(7, 3), (7, 4), (7, 5)]);
    assert_partitions(&plan.entries()[2], &[(7, 6), (7, 7)]);
}

#[test]
fn range_uses_each_topics_subscriber_denominator() {
    let members = joined(&[(1, &[1, 2]), (2, &[1]), (3, &[2])]);
    let plan = plan(&members, &[(1, 5), (2, 3)]);
    assert_partitions(
        &plan.entries()[0],
        &[(1, 0), (1, 1), (1, 2), (2, 0), (2, 1)],
    );
    assert_partitions(&plan.entries()[1], &[(1, 3), (1, 4)]);
    assert_partitions(&plan.entries()[2], &[(2, 2)]);
}

#[test]
fn range_retains_members_with_empty_assignments() {
    let members = joined(&[(1, &[4]), (2, &[4]), (3, &[4])]);
    let plan = plan(&members, &[(4, 2)]);
    assert_eq!(plan.entries().len(), 3);
    assert_partitions(&plan.entries()[0], &[(4, 0)]);
    assert_partitions(&plan.entries()[1], &[(4, 1)]);
    assert_partitions(&plan.entries()[2], &[]);
}

#[test]
fn zero_partition_topics_assign_nothing_and_unsubscribed_counts_reject() {
    let members = joined(&[(1, &[5]), (2, &[5])]);
    let empty = plan(&members, &[(5, 0)]);
    assert_partitions(&empty.entries()[0], &[]);
    assert_partitions(&empty.entries()[1], &[]);
    assert_eq!(
        ClassicAssignmentPlan::try_range(&members, &[count(5, 0), count(9, 2)]),
        Err(ClassicAssignmentError::UnsubscribedTopicCount(topic(9)))
    );
}

#[test]
fn range_local_lookup_uses_exact_member_after_complete_plan() {
    let members = joined(&[(1, &[1, 2]), (2, &[1]), (3, &[2])]);
    let plan = plan(&members, &[(1, 5), (2, 3)]);
    let local = plan
        .entries()
        .iter()
        .find(|assignment| assignment.slot() == slot(2))
        .unwrap_or_else(|| panic!("member two assignment"));
    assert_partitions(local, &[(1, 3), (1, 4)]);
}

#[test]
fn range_rejects_missing_ordering_and_capacity_before_a_plan_exists() {
    let members = joined(&[(1, &[1, 2])]);
    assert_eq!(
        ClassicAssignmentPlan::try_range(&members, &[count(1, 1)]),
        Err(ClassicAssignmentError::MissingTopicCount(topic(2)))
    );
    assert_eq!(
        ClassicAssignmentPlan::try_range(&members, &[count(2, 1), count(1, 1)]),
        Err(ClassicAssignmentError::OutOfOrderTopicCount)
    );
    assert_eq!(
        ClassicAssignmentPlan::try_range(&members, &[count(1, 1), count(1, 1)]),
        Err(ClassicAssignmentError::DuplicateTopicCount(topic(1)))
    );
    assert_eq!(
        ClassicAssignmentPlan::try_range(&members, &[count(1, 65), count(2, 0)]),
        Err(ClassicAssignmentError::MemberPartitionLimit {
            member_id: member(1),
            actual: 65,
        })
    );
}

fn joined(specification: &[(u32, &[u64])]) -> ClassicJoinMembers {
    let members = specification
        .iter()
        .map(|(raw, topics)| {
            let topics = topics.iter().copied().map(topic).collect();
            let subscription = ClassicSubscription::try_new(topics)
                .unwrap_or_else(|error| panic!("valid subscription: {error:?}"));
            ClassicJoinMember::new(
                slot(*raw),
                member(u64::from(*raw)),
                rank(*raw),
                subscription,
            )
        })
        .collect();
    ClassicJoinMembers::try_new(members)
        .unwrap_or_else(|error| panic!("valid join members: {error:?}"))
}

fn plan(members: &ClassicJoinMembers, counts: &[(u64, u32)]) -> super::ClassicAssignmentPlan {
    let counts = counts
        .iter()
        .map(|(topic_id, count_value)| count(*topic_id, *count_value))
        .collect::<Vec<_>>();
    ClassicAssignmentPlan::try_range(members, &counts)
        .unwrap_or_else(|error| panic!("valid Range plan: {error:?}"))
}

fn assert_partitions(assignment: &ClassicMemberAssignment, expected: &[(u64, u32)]) {
    let actual = assignment
        .partitions()
        .iter()
        .map(|partition| (partition.topic_id().get(), partition.partition().get()))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

const fn count(topic_id: u64, partitions: u32) -> TopicPartitionCount {
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

const fn topic(value: u64) -> TopicId {
    TopicId::from_raw(value)
}
