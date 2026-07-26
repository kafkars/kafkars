//! Exact scalar fence, position-result, and throttle representations.

use core::num::NonZeroI16;

use crate::{
    AssignmentGeneration, GroupAssignmentPartition, GroupId, MemberId, MembershipCycle,
    NextFetchOffset, PartitionIndex, TopicId,
};

use super::{
    GroupPositionBatch, GroupPositionBrokerError, GroupPositionFence, GroupPositionPartitionFact,
    GroupPositionPartitionResult,
};

#[test]
fn fence_retains_exact_group_membership_member_and_assignment_identity() {
    let fence = GroupPositionFence::new(group(7), cycle(11), member(13), generation(17));

    assert_eq!(fence.group_id(), group(7));
    assert_eq!(fence.membership_cycle(), cycle(11));
    assert_eq!(fence.member_id(), member(13));
    assert_eq!(fence.assignment_generation(), generation(17));
}

#[test]
fn committed_missing_and_rejected_facts_preserve_order_and_throttle() {
    let first = assigned(3, 0);
    let second = assigned(3, 2);
    let error = GroupPositionBrokerError::new(nonzero(-32_000));
    let batch = GroupPositionBatch::new(
        41,
        vec![
            GroupPositionPartitionFact::committed(first, offset(19)),
            GroupPositionPartitionFact::missing(second),
            GroupPositionPartitionFact::rejected(assigned(5, 0), error),
        ],
    );

    assert_eq!(batch.throttle_time_ms(), 41);
    assert_eq!(batch.facts()[0].partition(), first);
    assert_eq!(
        batch.facts()[0].result(),
        GroupPositionPartitionResult::Committed(offset(19))
    );
    assert_eq!(
        batch.facts()[1].result(),
        GroupPositionPartitionResult::Missing
    );
    assert_eq!(
        batch.facts()[2].result(),
        GroupPositionPartitionResult::Rejected(error)
    );
    assert_eq!(error.code(), -32_000);
    let (throttle, facts) = batch.into_parts();
    assert_eq!(throttle, 41);
    assert_eq!(facts.len(), 3);
}

fn group(value: u64) -> GroupId {
    GroupId::try_from_raw(value).unwrap_or_else(|| panic!("nonzero group"))
}

fn member(value: u64) -> MemberId {
    MemberId::try_from_raw(value).unwrap_or_else(|| panic!("nonzero member"))
}

fn generation(value: u64) -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(value).unwrap_or_else(|| panic!("nonzero generation"))
}

fn cycle(value: u64) -> MembershipCycle {
    MembershipCycle::try_from_raw(value).unwrap_or_else(|| panic!("nonzero cycle"))
}

fn assigned(topic: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}

fn nonzero(value: i16) -> NonZeroI16 {
    NonZeroI16::new(value).unwrap_or_else(|| panic!("nonzero broker code"))
}
