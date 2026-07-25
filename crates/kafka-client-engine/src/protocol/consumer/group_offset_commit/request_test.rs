//! Generated classic-group `OffsetCommit` request semantics.

use std::sync::Arc;

use kafka_wire::OffsetCommitRequest;
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    model_test::{entry, prepared, topic},
    request::group_offset_commit_request,
};

#[test]
fn request_uses_exact_spellings_next_offsets_epochs_and_empty_metadata() {
    let prepared = prepared(
        vec![
            entry(1, 0, 10, Some(7)),
            entry(1, 2, 30, None),
            entry(2, 1, 20, Some(8)),
        ],
        4,
        vec![topic(1, Arc::from("orders")), topic(2, Arc::from("audit"))],
    );
    let request = group_offset_commit_request(&prepared);

    assert_eq!(request.group_id.as_str(), "readers");
    assert_eq!(request.member_id.as_str(), "member-a");
    assert_eq!(request.generation_id_or_member_epoch, 4);
    assert_eq!(request.retention_time_ms, -1);
    assert!(request.group_instance_id.is_none());
    assert_eq!(request.topics.len(), 2);
    assert_eq!(request.topics[0].name.as_str(), "orders");
    assert_eq!(request.topics[1].name.as_str(), "audit");
    assert_eq!(
        request.topics[0]
            .partitions
            .iter()
            .map(|partition| (
                partition.partition_index,
                partition.committed_offset,
                partition.committed_leader_epoch,
                partition
                    .committed_metadata
                    .as_ref()
                    .map(kafka_wire_core::StrBytes::as_str),
            ))
            .collect::<Vec<_>>(),
        vec![(0, 10, 7, Some("")), (2, 30, -1, Some(""))]
    );
    assert_round_trip(&request, 6);
    assert_round_trip(&request, 9);
}

#[test]
fn absent_leader_epoch_does_not_require_v6() {
    let prepared = prepared(
        vec![entry(1, 0, 10, None)],
        4,
        vec![topic(1, Arc::from("orders"))],
    );
    assert!(!prepared.requires_leader_epoch());
    let request = group_offset_commit_request(&prepared);
    assert_eq!(request.topics[0].partitions[0].committed_leader_epoch, -1);
    assert_round_trip(&request, 2);
}

fn assert_round_trip(request: &OffsetCommitRequest, version: i16) {
    let version = ApiVersion::new(version);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("generated v{version} request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("generated request frame is bounded: {error}"));
    let decoded = OffsetCommitRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("generated v{version} request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("generated request consumes its frame: {error}"));
    assert_eq!(decoded.group_id.as_str(), "readers");
    assert_eq!(decoded.member_id.as_str(), "member-a");
}
