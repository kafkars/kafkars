//! Generated explicit-topic request selection across the v7/v8 schema split.

use std::sync::Arc;

use kafka_wire::OffsetFetchRequest;
use kafka_wire_core::{
    ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode, Uuid,
};

use super::{
    preparation::{GroupOffsetFetchPreparation, prepare_group_offset_fetch_request},
    preparation_test::topic,
};

#[test]
fn every_legacy_version_encodes_only_explicit_assigned_partitions() {
    for version in 2..=7 {
        let decoded = encoded_request(version);
        assert_eq!(decoded.group_id.as_str(), "readers");
        let topics = decoded
            .topics
            .unwrap_or_else(|| panic!("v{version} must not select nullable all-topics"));
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0].name.as_str(), "z");
        assert_eq!(topics[0].partition_indexes, [2, 0]);
        assert_eq!(topics[1].name.as_str(), "a");
        assert_eq!(topics[1].partition_indexes, [1]);
        assert!(decoded.groups.is_empty());
        assert!(!decoded.require_stable);
    }
}

#[test]
fn every_modern_version_encodes_one_explicit_matching_group() {
    for version in 8..=9 {
        let decoded = encoded_request(version);
        assert!(decoded.group_id.is_empty());
        assert!(decoded.topics.is_some_and(|topics| topics.is_empty()));
        assert_eq!(decoded.groups.len(), 1);
        let group = &decoded.groups[0];
        assert_eq!(group.group_id.as_str(), "readers");
        assert!(group.member_id.is_none());
        assert_eq!(group.member_epoch, -1);
        let topics = group
            .topics
            .as_ref()
            .unwrap_or_else(|| panic!("v{version} must not select nullable all-topics"));
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0].name.as_str(), "z");
        assert_eq!(topics[0].topic_id, Uuid::ZERO);
        assert_eq!(topics[0].partition_indexes, [2, 0]);
        assert!(!decoded.require_stable);
    }
}

#[test]
fn request_support_is_exactly_v2_through_v9() {
    let request = wire_request();
    for version in 2..=9 {
        assert!(request.encoded_len(ApiVersion::new(version)).is_ok());
    }
    assert!(request.encoded_len(ApiVersion::new(1)).is_err());
    assert!(request.encoded_len(ApiVersion::new(10)).is_err());
}

fn encoded_request(version: i16) -> OffsetFetchRequest {
    let request = wire_request();
    let version = ApiVersion::new(version);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("v{version} request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request is bounded: {error}"));
    let decoded = OffsetFetchRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("v{version} request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}

fn wire_request() -> super::request::GroupOffsetFetchRequest {
    let GroupOffsetFetchPreparation::Prepared(prepared) = prepare_group_offset_fetch_request(
        Arc::from("readers"),
        vec![topic("z", &[2, 0]), topic("a", &[1])],
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("valid assignment: {error:?}")) else {
        panic!("nonempty assignment must prepare");
    };
    let (_, request) = prepared.into_parts();
    request.into_wire_request()
}
