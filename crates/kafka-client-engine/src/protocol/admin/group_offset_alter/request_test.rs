//! Generated `OffsetCommit` request, sentinel, and grouping scenarios.

use kafka_wire::{KafkaMessage, OffsetCommitRequest};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    OffsetCommitTargetRef,
    request::{GroupOffsetAlterRequestFailure, group_offset_alter_request},
};

#[test]
fn request_groups_topics_and_writes_exact_non_member_sentinels() {
    let targets = [
        OffsetCommitTargetRef::new("orders", 2, 91, None, Some("first")),
        OffsetCommitTargetRef::new("audit", 7, 13, None, None),
        OffsetCommitTargetRef::new("orders", 1, 42, None, Some("")),
    ];
    let request = group_offset_alter_request("readers", &targets, None, usize::MAX)
        .unwrap_or_else(|error| panic!("charged request: {error:?}"));
    let decoded = round_trip(&request, ApiVersion::new(2));

    assert_eq!(decoded.group_id.as_str(), "readers");
    assert_eq!(decoded.generation_id_or_member_epoch, -1);
    assert!(decoded.member_id.is_empty());
    assert_eq!(decoded.group_instance_id, None);
    assert_eq!(decoded.retention_time_ms, -1);
    assert_eq!(decoded.topics.len(), 2);
    assert_eq!(decoded.topics[0].name.as_str(), "audit");
    assert_eq!(decoded.topics[1].name.as_str(), "orders");
    assert_eq!(
        decoded.topics[1]
            .partitions
            .iter()
            .map(|partition| (
                partition.partition_index,
                partition.committed_offset,
                partition
                    .committed_metadata
                    .as_ref()
                    .map(kafka_wire_core::StrBytes::as_str),
            ))
            .collect::<Vec<_>>(),
        [(2, 91, Some("first")), (1, 42, Some(""))]
    );
}

#[test]
fn explicit_retention_is_exact_through_v4_and_absent_afterward() {
    let targets = [OffsetCommitTargetRef::new("orders", 2, 91, None, None)];
    let request = group_offset_alter_request("readers", &targets, Some(86_400_000), usize::MAX)
        .unwrap_or_else(|error| panic!("charged request: {error:?}"));

    assert_eq!(request.retention_time_ms, 86_400_000);
    assert_eq!(
        round_trip(&request, ApiVersion::new(4)).retention_time_ms,
        86_400_000
    );
    assert_eq!(
        round_trip(&request, ApiVersion::new(5)).retention_time_ms,
        -1
    );
}

#[test]
fn v9_round_trip_preserves_optional_epoch_and_nullable_metadata() {
    let targets = [
        OffsetCommitTargetRef::new("orders", 2, 91, Some(8), Some("")),
        OffsetCommitTargetRef::new("orders", 3, 92, None, None),
    ];
    let request = group_offset_alter_request("readers", &targets, None, usize::MAX)
        .unwrap_or_else(|error| panic!("charged request: {error:?}"));
    let decoded = round_trip(&request, ApiVersion::new(9));
    let partitions = &decoded.topics[0].partitions;

    assert_eq!(partitions[0].committed_leader_epoch, 8);
    assert_eq!(
        partitions[0]
            .committed_metadata
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("")
    );
    assert_eq!(partitions[1].committed_leader_epoch, -1);
    assert_eq!(partitions[1].committed_metadata, None);
}

#[test]
fn generated_message_supports_our_exact_v2_through_v9_subset() {
    assert!(OffsetCommitRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(2)));
    assert!(OffsetCommitRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(9)));
    assert!(OffsetCommitRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(10)));
}

#[test]
fn request_grouping_scratch_is_proven_before_allocation() {
    let target = [OffsetCommitTargetRef::new("orders", 1, 4, None, None)];
    assert_eq!(
        group_offset_alter_request("readers", &target, None, 0).err(),
        Some(GroupOffsetAlterRequestFailure::RetainedBytes)
    );
}

fn round_trip(request: &OffsetCommitRequest, version: ApiVersion) -> OffsetCommitRequest {
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = OffsetCommitRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
