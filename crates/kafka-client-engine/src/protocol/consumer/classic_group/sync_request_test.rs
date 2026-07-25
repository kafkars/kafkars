//! Core-plan correlation and generated Sync request construction scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ClassicAssignmentPlan, ClassicGeneration, ClassicJoinMember, ClassicJoinMembers,
    ClassicSubscription, GroupId, JoinedMemberSlot, MemberId, MemberRank, Moment, TopicId,
    TopicPartitionCount,
};
use kafka_wire::{
    SYNC_GROUP_API_DESCRIPTOR, SyncGroupRequest, decode_consumer_protocol_assignment,
};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    ClassicSyncMember, ClassicSyncRequestFailure, ClassicSyncTopic, classic_sync_group_request,
};

fn slot(value: u32) -> JoinedMemberSlot {
    JoinedMemberSlot::try_from_raw(value).unwrap_or_else(|| panic!("slot"))
}

fn generation() -> ClassicGeneration {
    ClassicGeneration::try_from_raw(7).unwrap_or_else(|| panic!("generation"))
}

fn follower_plan() -> ClassicAssignmentPlan {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group"));
    let timing = kafka_client_core::ClassicGroupTiming::try_new(10_000, 30_000)
        .unwrap_or_else(|error| panic!("timing: {error}"));
    let heartbeat =
        kafka_client_core::ClassicHeartbeatPolicy::try_new(1_000_000_000, 2_000_000_000)
            .unwrap_or_else(|error| panic!("heartbeat policy: {error}"));
    let mut machine = kafka_client_core::ClassicGroupMachine::new(group_id, timing, heartbeat);
    let cycle = machine
        .apply(kafka_client_core::ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Moment::from_tick(1)
                .checked_deadline_after(10)
                .unwrap_or_else(|| panic!("deadline")),
        })
        .unwrap_or_else(|error| panic!("begin failed: {error}"))
        .into_effects()
        .next()
        .and_then(|effect| match effect {
            kafka_client_core::ClassicGroupEffect::Join { cycle, .. } => Some(cycle),
            _ => None,
        })
        .unwrap_or_else(|| panic!("Join effect"));
    machine
        .apply(kafka_client_core::ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id: MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member")),
            generation: generation(),
        })
        .unwrap_or_else(|error| panic!("follower Join failed: {error}"))
        .into_effects()
        .next()
        .and_then(|effect| match effect {
            kafka_client_core::ClassicGroupEffect::Sync { plan, .. } => Some(plan),
            _ => None,
        })
        .unwrap_or_else(|| panic!("Sync effect"))
}

fn leader_plan() -> ClassicAssignmentPlan {
    let subscription = || {
        ClassicSubscription::try_new(vec![TopicId::from_raw(1)])
            .unwrap_or_else(|error| panic!("subscription failed: {error:?}"))
    };
    let members = ClassicJoinMembers::try_new(vec![
        ClassicJoinMember::new(
            slot(1),
            MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member")),
            MemberRank::try_from_raw(1).unwrap_or_else(|| panic!("rank")),
            subscription(),
        ),
        ClassicJoinMember::new(
            slot(2),
            MemberId::try_from_raw(2).unwrap_or_else(|| panic!("member")),
            MemberRank::try_from_raw(2).unwrap_or_else(|| panic!("rank")),
            subscription(),
        ),
    ])
    .unwrap_or_else(|error| panic!("members failed: {error:?}"));
    ClassicAssignmentPlan::try_range(
        &members,
        &[TopicPartitionCount::new(TopicId::from_raw(1), 3)],
    )
    .unwrap_or_else(|error| panic!("Range plan failed: {error:?}"))
}

#[test]
fn follower_request_has_an_empty_assignment_plan() {
    let prepared = classic_sync_group_request(
        "workers",
        "member-b",
        generation(),
        follower_plan(),
        &[],
        &[],
    )
    .unwrap_or_else(|error| panic!("follower Sync failed: {error:?}"));
    let request = prepared.request_for_test();
    assert_eq!(request.group_id.as_str(), "workers");
    assert_eq!(request.member_id.as_str(), "member-b");
    assert_eq!(request.generation_id, 7);
    assert_eq!(request.group_instance_id, None);
    assert_eq!(request.protocol_type, None);
    assert_eq!(request.protocol_name, None);
    assert!(request.assignments.is_empty());
}

#[test]
fn leader_plan_is_correlated_by_slot_and_topic_identity() {
    let prepared = classic_sync_group_request(
        "workers",
        "member-a",
        generation(),
        leader_plan(),
        &[
            ClassicSyncMember::new(slot(2), Arc::from("member-b")),
            ClassicSyncMember::new(slot(1), Arc::from("member-a")),
        ],
        &[ClassicSyncTopic::new(
            TopicId::from_raw(1),
            Arc::from("orders"),
        )],
    )
    .unwrap_or_else(|error| panic!("leader Sync failed: {error:?}"));
    let request = prepared.request_for_test();
    assert_eq!(request.assignments.len(), 2);
    assert_eq!(request.assignments[0].member_id.as_str(), "member-a");
    assert_eq!(request.assignments[1].member_id.as_str(), "member-b");
    let decoded = request
        .assignments
        .iter()
        .map(|assignment| {
            decode_consumer_protocol_assignment(
                assignment.assignment.clone(),
                DecodeLimits::default(),
            )
            .unwrap_or_else(|error| panic!("assignment decode failed: {error}"))
        })
        .collect::<Vec<_>>();
    assert!(decoded.iter().all(|(version, _)| version.value() == 0));
    assert_eq!(decoded[0].1.assigned_partitions[0].topic.as_str(), "orders");
    assert_eq!(decoded[0].1.assigned_partitions[0].partitions, [0, 1]);
    assert_eq!(decoded[1].1.assigned_partitions[0].partitions, [2]);
    assert!(decoded.iter().all(|(_, value)| value.user_data.is_none()));
}

#[test]
fn generated_request_round_trips_at_both_exact_driver_bounds() {
    let prepared = classic_sync_group_request(
        "workers",
        "member-b",
        generation(),
        follower_plan(),
        &[],
        &[],
    )
    .unwrap_or_else(|error| panic!("Sync request failed: {error:?}"));
    let request = prepared.request_for_test();
    for version in [ApiVersion::new(0), ApiVersion::new(2)] {
        assert!(
            SYNC_GROUP_API_DESCRIPTOR
                .supported_versions
                .contains(version)
        );
        let mut encoded = BytesMut::new();
        request
            .encode_into(&mut encoded, version)
            .unwrap_or_else(|error| panic!("Sync encode failed: {error}"));
        let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
            .unwrap_or_else(|error| panic!("decoder failed: {error}"));
        let decoded = SyncGroupRequest::decode(&mut decoder, version)
            .unwrap_or_else(|error| panic!("Sync decode failed: {error}"));
        decoder
            .finish()
            .unwrap_or_else(|error| panic!("Sync trailing bytes: {error}"));
        assert_eq!(&decoded, request);
    }
}

#[test]
fn member_and_topic_correlations_are_exact() {
    let missing = classic_sync_group_request(
        "workers",
        "member-a",
        generation(),
        leader_plan(),
        &[ClassicSyncMember::new(slot(1), Arc::from("member-a"))],
        &[ClassicSyncTopic::new(
            TopicId::from_raw(1),
            Arc::from("orders"),
        )],
    );
    assert_eq!(
        missing.err(),
        Some(ClassicSyncRequestFailure::MissingMember(slot(2)))
    );
    let duplicate = classic_sync_group_request(
        "workers",
        "member-a",
        generation(),
        leader_plan(),
        &[
            ClassicSyncMember::new(slot(1), Arc::from("member-a")),
            ClassicSyncMember::new(slot(1), Arc::from("member-b")),
        ],
        &[ClassicSyncTopic::new(
            TopicId::from_raw(1),
            Arc::from("orders"),
        )],
    );
    assert_eq!(
        duplicate.err(),
        Some(ClassicSyncRequestFailure::DuplicateMemberSlot(slot(1)))
    );
    let topic = classic_sync_group_request(
        "workers",
        "member-a",
        generation(),
        leader_plan(),
        &[
            ClassicSyncMember::new(slot(1), Arc::from("member-a")),
            ClassicSyncMember::new(slot(2), Arc::from("member-b")),
        ],
        &[],
    );
    assert_eq!(
        topic.err(),
        Some(ClassicSyncRequestFailure::MissingTopic(TopicId::from_raw(
            1
        )))
    );
}

#[test]
fn follower_rejects_an_unowned_assignment_mapping() {
    assert_eq!(
        classic_sync_group_request(
            "workers",
            "member-b",
            generation(),
            follower_plan(),
            &[ClassicSyncMember::new(slot(1), Arc::from("member-b"))],
            &[],
        )
        .err(),
        Some(ClassicSyncRequestFailure::UnexpectedMember(slot(1)))
    );
}
