//! Shared catalog, deadline, and completed-position fixtures for group Fetch tests.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, GroupPositionBatch,
    GroupPositionFence, GroupPositionPartitionFact, MemberId, MembershipCycle, Moment,
    NextFetchOffset, PartitionIndex, TopicId,
};

use super::super::{
    classic_group_position::{
        ClassicGroupPositionCompleted, test_support::completed_ready as completed_position_ready,
    },
    session_catalog::GroupSessionCatalog,
};

pub(super) const ATTEMPT_TIMEOUT_TICKS: u64 = 30_000_000_000;

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

pub(super) fn catalog(topics: &[&str]) -> GroupSessionCatalog {
    let topics = topics
        .iter()
        .map(|topic| Arc::<str>::from(*topic))
        .collect::<Vec<_>>();
    GroupSessionCatalog::try_new(
        GroupId::try_from_raw(3).unwrap_or_else(|| panic!("group identity")),
        Arc::from("workers"),
        &topics,
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"))
}

pub(super) fn assert_attempt_deadline(deadline: Deadline, before: Moment, after: Moment) {
    assert!(deadline.tick() >= before.tick() + ATTEMPT_TIMEOUT_TICKS);
    assert!(deadline.tick() <= after.tick() + ATTEMPT_TIMEOUT_TICKS);
}
