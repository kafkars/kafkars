//! Cooperative-sticky balance, retention, and two-round handoff tests.

#![expect(
    clippy::cast_possible_truncation,
    clippy::type_complexity,
    reason = "bounded fixture tables mirror the assignment domain directly"
)]

use crate::{GroupAssignmentPartition, MemberId, PartitionIndex, TopicId};

use super::{
    ClassicAssignmentError, ClassicAssignmentPlan, ClassicGeneration, ClassicJoinMember,
    ClassicJoinMembers, ClassicSubscription, JoinedMemberSlot, MemberRank, TopicPartitionCount,
};

#[test]
fn cooperative_plan_balances_an_initial_join_deterministically() {
    let members = members(&[(&[0], &[]), (&[0], &[])]);

    let plan = ClassicAssignmentPlan::try_cooperative_sticky(&members, &[count(0, 4)])
        .unwrap_or_else(|error| panic!("cooperative plan failed: {error:?}"));

    assert_eq!(raw_partitions(&plan, 0), vec![(0, 0), (0, 2)]);
    assert_eq!(raw_partitions(&plan, 1), vec![(0, 1), (0, 3)]);
}

#[test]
fn cooperative_plan_withholds_transfers_until_the_second_round() {
    let first = members(&[(&[0], &[(0, 0), (0, 1), (0, 2), (0, 3)]), (&[0], &[])]);
    let first_plan = ClassicAssignmentPlan::try_cooperative_sticky(&first, &[count(0, 4)])
        .unwrap_or_else(|error| panic!("first cooperative plan failed: {error:?}"));
    assert_eq!(raw_partitions(&first_plan, 0), vec![(0, 0), (0, 1)]);
    assert!(raw_partitions(&first_plan, 1).is_empty());
    assert_eq!(first_plan.withheld_transfers(), 2);
    assert!(first_plan.requires_followup());

    let second = members(&[(&[0], &[(0, 0), (0, 1)]), (&[0], &[])]);
    let second_plan = ClassicAssignmentPlan::try_cooperative_sticky(&second, &[count(0, 4)])
        .unwrap_or_else(|error| panic!("second cooperative plan failed: {error:?}"));
    assert_eq!(raw_partitions(&second_plan, 0), vec![(0, 0), (0, 1)]);
    assert_eq!(raw_partitions(&second_plan, 1), vec![(0, 2), (0, 3)]);
    assert!(!second_plan.requires_followup());
}

#[test]
fn cooperative_rebalance_grows_multiple_empty_recipients_for_repeated_transfers() {
    let first = members(&[
        (&[0], &[(0, 0), (0, 1), (0, 2), (0, 3), (0, 4), (0, 5)]),
        (&[0], &[]),
        (&[0], &[]),
    ]);
    let first_plan = ClassicAssignmentPlan::try_cooperative_sticky(&first, &[count(0, 6)])
        .unwrap_or_else(|error| panic!("first cooperative plan failed: {error:?}"));

    assert_eq!(raw_partitions(&first_plan, 0), vec![(0, 0), (0, 1)]);
    assert!(raw_partitions(&first_plan, 1).is_empty());
    assert!(raw_partitions(&first_plan, 2).is_empty());
    assert_eq!(first_plan.withheld_transfers(), 4);

    let second = members(&[(&[0], &[(0, 0), (0, 1)]), (&[0], &[]), (&[0], &[])]);
    let second_plan = ClassicAssignmentPlan::try_cooperative_sticky(&second, &[count(0, 6)])
        .unwrap_or_else(|error| panic!("second cooperative plan failed: {error:?}"));

    assert_eq!(raw_partitions(&second_plan, 0), vec![(0, 0), (0, 1)]);
    assert_eq!(raw_partitions(&second_plan, 1), vec![(0, 2), (0, 4)]);
    assert_eq!(raw_partitions(&second_plan, 2), vec![(0, 3), (0, 5)]);
    assert!(!second_plan.requires_followup());
}

#[test]
fn cooperative_plan_keeps_an_already_balanced_assignment_sticky() {
    let members = members(&[(&[0], &[(0, 0), (0, 2)]), (&[0], &[(0, 1), (0, 3)])]);

    let plan = ClassicAssignmentPlan::try_cooperative_sticky(&members, &[count(0, 4)])
        .unwrap_or_else(|error| panic!("stable cooperative plan failed: {error:?}"));

    assert_eq!(raw_partitions(&plan, 0), vec![(0, 0), (0, 2)]);
    assert_eq!(raw_partitions(&plan, 1), vec![(0, 1), (0, 3)]);
}

#[test]
fn cooperative_plan_rejects_ambiguous_current_ownership() {
    let members = members(&[(&[0], &[(0, 0)]), (&[0], &[(0, 0)])]);

    assert_eq!(
        ClassicAssignmentPlan::try_cooperative_sticky(&members, &[count(0, 1)]),
        Err(ClassicAssignmentError::ConflictingOwnedPartition(
            partition(0, 0)
        ))
    );
}

#[test]
fn cooperative_plan_uses_the_highest_reported_generation_for_duplicate_ownership() {
    let members = members_with_generations(&[(&[0], &[(0, 0)], 1), (&[0], &[(0, 0)], 2)]);

    let plan = ClassicAssignmentPlan::try_cooperative_sticky(&members, &[count(0, 1)])
        .unwrap_or_else(|error| panic!("generation resolution failed: {error:?}"));

    assert!(raw_partitions(&plan, 0).is_empty());
    assert_eq!(raw_partitions(&plan, 1), vec![(0, 0)]);
}

#[test]
fn cooperative_plan_respects_heterogeneous_subscriptions() {
    let members = members(&[(&[0], &[(0, 0), (0, 1)]), (&[0, 1], &[(1, 0)]), (&[1], &[])]);

    let plan = ClassicAssignmentPlan::try_cooperative_sticky(&members, &[count(0, 2), count(1, 2)])
        .unwrap_or_else(|error| panic!("heterogeneous cooperative plan failed: {error:?}"));

    assert_eq!(raw_partitions(&plan, 0), vec![(0, 0), (0, 1)]);
    assert_eq!(raw_partitions(&plan, 1), vec![(1, 0)]);
    assert_eq!(raw_partitions(&plan, 2), vec![(1, 1)]);
}

#[test]
fn cooperative_plan_revokes_dropped_topic_ownership_before_transfer() {
    let first = members(&[(&[1], &[(0, 0)]), (&[0], &[])]);
    let first_plan =
        ClassicAssignmentPlan::try_cooperative_sticky(&first, &[count(0, 1), count(1, 0)])
            .unwrap_or_else(|error| panic!("dropped-topic plan failed: {error:?}"));

    assert!(raw_partitions(&first_plan, 0).is_empty());
    assert!(raw_partitions(&first_plan, 1).is_empty());
    assert_eq!(first_plan.withheld_transfers(), 1);

    let second = members(&[(&[1], &[]), (&[0], &[])]);
    let second_plan =
        ClassicAssignmentPlan::try_cooperative_sticky(&second, &[count(0, 1), count(1, 0)])
            .unwrap_or_else(|error| panic!("released-topic plan failed: {error:?}"));

    assert!(raw_partitions(&second_plan, 0).is_empty());
    assert_eq!(raw_partitions(&second_plan, 1), vec![(0, 0)]);
    assert!(!second_plan.requires_followup());
}

fn members(specifications: &[(&[u64], &[(u64, u32)])]) -> ClassicJoinMembers {
    let with_generations: Vec<_> = specifications
        .iter()
        .map(|(topics, owned)| (*topics, *owned, 1))
        .collect();
    members_with_generations(&with_generations)
}

fn members_with_generations(specifications: &[(&[u64], &[(u64, u32)], i32)]) -> ClassicJoinMembers {
    let mut joined = Vec::new();
    for (index, (topics, owned, generation)) in specifications.iter().enumerate() {
        let slot = JoinedMemberSlot::try_from_raw((index + 1) as u32)
            .unwrap_or_else(|| panic!("nonzero member slot"));
        let rank = MemberRank::try_from_raw((index + 1) as u32)
            .unwrap_or_else(|| panic!("nonzero member rank"));
        let member_id = MemberId::try_from_raw((index + 1) as u64)
            .unwrap_or_else(|| panic!("nonzero member id"));
        let subscription = ClassicSubscription::try_new_with_owned(
            topics.iter().copied().map(TopicId::from_raw).collect(),
            owned
                .iter()
                .map(|(topic, partition)| {
                    super::cooperative_sticky_test::partition(*topic, *partition)
                })
                .collect(),
            Some(
                ClassicGeneration::try_from_raw(*generation)
                    .unwrap_or_else(|| panic!("nonnegative generation")),
            ),
        )
        .unwrap_or_else(|error| panic!("valid cooperative subscription: {error:?}"));
        joined.push(ClassicJoinMember::new(slot, member_id, rank, subscription));
    }
    ClassicJoinMembers::try_new(joined)
        .unwrap_or_else(|error| panic!("valid joined members: {error:?}"))
}

fn count(topic: u64, partitions: u32) -> TopicPartitionCount {
    TopicPartitionCount::new(TopicId::from_raw(topic), partitions)
}

fn partition(topic: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

fn raw_partitions(plan: &ClassicAssignmentPlan, member: usize) -> Vec<(u64, u32)> {
    plan.entries()[member]
        .partitions()
        .iter()
        .map(|partition| (partition.topic_id().get(), partition.partition().get()))
        .collect()
}
