//! Shared completed-position fixtures for group Fetch activation tests.

use kafka_client_core::{
    AssignmentGeneration, GroupAssignmentPartition, GroupId, GroupPositionBatch,
    GroupPositionFence, GroupPositionPartitionFact, MemberId, MembershipCycle, Moment,
    NextFetchOffset, PartitionIndex, TopicId,
};

use super::super::classic_group_position::{
    ClassicGroupPositionCompleted, test_support::completed_ready as completed_position_ready,
};

pub(super) fn completed_ready(
    fence: GroupPositionFence,
    observed_at: Moment,
    throttle_time_ms: u32,
    facts: Vec<GroupPositionPartitionFact>,
) -> ClassicGroupPositionCompleted {
    completed_position_ready(
        fence,
        observed_at,
        GroupPositionBatch::new(throttle_time_ms, facts),
    )
}

pub(super) fn committed(topic: u64, partition: u32, offset: i64) -> GroupPositionPartitionFact {
    GroupPositionPartitionFact::committed(
        GroupAssignmentPartition::new(
            TopicId::from_raw(topic),
            PartitionIndex::from_raw(partition),
        ),
        NextFetchOffset::try_from_raw(offset).unwrap_or_else(|| panic!("next offset")),
    )
}

pub(super) fn position_fence(generation: u64) -> GroupPositionFence {
    GroupPositionFence::new(
        GroupId::try_from_raw(3).unwrap_or_else(|| panic!("group")),
        MembershipCycle::initial(),
        MemberId::try_from_raw(5).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("assignment generation")),
    )
}
