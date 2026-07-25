//! Maximum bounded Sync assignment round-trip through generated wire DTOs.

use std::sync::Arc;

use kafka_client_core::{
    ClassicAssignmentPlan, ClassicGeneration, ClassicJoinMember, ClassicJoinMembers,
    ClassicSubscription, JoinedMemberSlot, MemberId, MemberRank, TopicId, TopicPartitionCount,
};
use kafka_wire::SyncGroupResponse;

use super::{
    ClassicSyncMember, ClassicSyncOutcome, ClassicSyncTopic, classic_sync_group_request,
    normalize_classic_sync_response,
    validation::{MAX_INNER_PAYLOAD_BYTES, MAX_MEMBER_PARTITIONS, MAX_TOPIC_BYTES, MAX_TOPICS},
};

#[test]
fn maximum_accepted_assignment_round_trips_through_the_decode_bound() {
    let topics = (0..MAX_TOPICS)
        .map(|index| Arc::<str>::from(format!("{index:02}-{}", "x".repeat(MAX_TOPIC_BYTES - 3))))
        .collect::<Vec<_>>();
    let topic_ids = (1..=MAX_TOPICS)
        .map(|raw| TopicId::from_raw(raw as u64))
        .collect::<Vec<_>>();
    let subscription = ClassicSubscription::try_new(topic_ids.clone())
        .unwrap_or_else(|error| panic!("maximum subscription failed: {error:?}"));
    let slot = JoinedMemberSlot::try_from_raw(1).unwrap_or_else(|| panic!("slot"));
    let members = ClassicJoinMembers::try_new(vec![ClassicJoinMember::new(
        slot,
        MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member")),
        MemberRank::try_from_raw(1).unwrap_or_else(|| panic!("rank")),
        subscription,
    )])
    .unwrap_or_else(|error| panic!("maximum members failed: {error:?}"));
    let counts = topic_ids
        .iter()
        .copied()
        .map(|topic_id| TopicPartitionCount::new(topic_id, 1))
        .collect::<Vec<_>>();
    let plan = ClassicAssignmentPlan::try_range(&members, &counts)
        .unwrap_or_else(|error| panic!("maximum plan failed: {error:?}"));
    let topic_map = topic_ids
        .into_iter()
        .zip(topics)
        .map(|(topic_id, topic)| ClassicSyncTopic::new(topic_id, topic))
        .collect::<Vec<_>>();
    let request = classic_sync_group_request(
        "workers",
        "member-a",
        ClassicGeneration::try_from_raw(7).unwrap_or_else(|| panic!("generation")),
        plan,
        &[ClassicSyncMember::new(slot, Arc::from("member-a"))],
        &topic_map,
    )
    .unwrap_or_else(|error| panic!("maximum Sync request failed: {error:?}"));
    let mut response = SyncGroupResponse::default();
    response.assignment = request.assignments[0].assignment.clone();
    assert_eq!(response.assignment.len(), MAX_INNER_PAYLOAD_BYTES);
    let ClassicSyncOutcome::Assigned { partitions, .. } =
        normalize_classic_sync_response(2, &response)
            .unwrap_or_else(|error| panic!("maximum Sync response failed: {error:?}"))
    else {
        panic!("assigned outcome expected");
    };
    assert_eq!(partitions.len(), MAX_MEMBER_PARTITIONS);
}
