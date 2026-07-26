//! Generated v0 request construction and grouping scenarios.

use kafka_wire::{KafkaMessage, OffsetDeleteRequest};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{OffsetDeleteTargetRef, group_offset_delete_request};

#[test]
fn request_reuses_generated_v0_and_groups_repeated_topics() {
    let targets = [
        OffsetDeleteTargetRef::new("orders", 2),
        OffsetDeleteTargetRef::new("audit", 7),
        OffsetDeleteTargetRef::new("orders", 1),
    ];
    let request = group_offset_delete_request("readers", &targets, usize::MAX)
        .unwrap_or_else(|error| panic!("charged request: {error:?}"));
    let decoded = round_trip(&request);
    assert_eq!(decoded.group_id.as_str(), "readers");
    assert_eq!(decoded.topics.len(), 2);
    assert_eq!(decoded.topics[0].name.as_str(), "audit");
    assert_eq!(decoded.topics[0].partitions[0].partition_index, 7);
    assert_eq!(decoded.topics[1].name.as_str(), "orders");
    assert_eq!(
        decoded.topics[1]
            .partitions
            .iter()
            .map(|partition| partition.partition_index)
            .collect::<Vec<_>>(),
        [2, 1]
    );
}

#[test]
fn generated_message_declares_only_exact_v0() {
    assert!(OffsetDeleteRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(0)));
    assert!(!OffsetDeleteRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(1)));
}

#[test]
fn request_grouping_scratch_is_proven_before_allocation() {
    let target = [OffsetDeleteTargetRef::new("orders", 1)];
    assert_eq!(
        group_offset_delete_request("readers", &target, 0).err(),
        Some(super::GroupOffsetDeleteRequestFailure::RetainedBytes)
    );
}

fn round_trip(request: &OffsetDeleteRequest) -> OffsetDeleteRequest {
    let version = ApiVersion::new(0);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("v0 request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = OffsetDeleteRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("v0 request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
