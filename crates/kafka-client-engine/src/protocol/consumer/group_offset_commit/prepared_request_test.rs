//! Pre-core generated request reservation and retained-byte scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, GroupCheckpoint, GroupCheckpointEntry, GroupId, MemberId, PartitionIndex,
    TopicId,
};
use kafka_wire::RetainedSize;

use super::{
    ClassicGroupCommitSession, GroupOffsetCommitTopicName, PreparedGroupOffsetCommitRequest,
};

#[test]
fn generated_request_is_complete_and_exactly_charged_before_core_admission() {
    let session = ClassicGroupCommitSession::new(
        group_id(1),
        Arc::from("group"),
        member_id(2),
        Arc::from("member"),
        generation(3),
        4,
    );
    let checkpoint = GroupCheckpoint::try_new(
        group_id(1),
        member_id(2),
        generation(3),
        vec![entry(1, 0, 11), entry(1, 1, 12), entry(2, 0, 21)],
    )
    .unwrap_or_else(|error| panic!("checkpoint: {error}"));
    let prepared = PreparedGroupOffsetCommitRequest::try_new(
        &session,
        &checkpoint,
        &[
            GroupOffsetCommitTopicName::new(TopicId::from_raw(1), Arc::from("orders")),
            GroupOffsetCommitTopicName::new(TopicId::from_raw(2), Arc::from("payments")),
        ],
    )
    .unwrap_or_else(|error| panic!("request preparation: {error:?}"));
    let request = prepared.request_for_test();

    assert_eq!(request.group_id.as_str(), "group");
    assert_eq!(request.member_id.as_str(), "member");
    assert_eq!(request.generation_id_or_member_epoch, 4);
    assert_eq!(request.topics.len(), 2);
    assert_eq!(request.topics[0].partitions.len(), 2);
    assert_eq!(request.topics[1].partitions.len(), 1);
    assert_eq!(
        prepared.retained_bytes(),
        request.retained_size().heap_bytes()
    );
}

fn entry(topic: u64, partition: u32, next_offset: i64) -> GroupCheckpointEntry {
    GroupCheckpointEntry::try_new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
        next_offset,
        None,
    )
    .unwrap_or_else(|error| panic!("entry: {error}"))
}

fn group_id(value: u64) -> GroupId {
    GroupId::try_from_raw(value).unwrap_or_else(|| panic!("group id"))
}

fn member_id(value: u64) -> MemberId {
    MemberId::try_from_raw(value).unwrap_or_else(|| panic!("member id"))
}

fn generation(value: u64) -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(value).unwrap_or_else(|| panic!("generation"))
}
