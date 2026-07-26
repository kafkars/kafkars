//! Generated request-shape selection across the `OffsetFetch` v7/v8 boundary.

use kafka_wire::OffsetFetchRequest;
use kafka_wire_core::{
    ApiVersion, BytesMut, DecodeLimits, Decoder, EncodeError, KafkaDecode, KafkaEncode,
};

use super::request::group_offsets_request;

#[test]
fn legacy_versions_encode_one_group_with_nullable_all_topics() {
    for version in [2, 6, 7] {
        let decoded = encoded_request(false, version);
        assert_eq!(decoded.group_id.as_str(), "readers");
        assert_eq!(decoded.topics, None);
        assert!(decoded.groups.is_empty());
        assert!(!decoded.require_stable);
    }
}

#[test]
fn modern_versions_encode_one_matching_group_with_nullable_all_topics() {
    for version in [8, 9] {
        let decoded = encoded_request(false, version);
        assert!(decoded.group_id.is_empty());
        assert_eq!(decoded.groups.len(), 1);
        assert_eq!(decoded.groups[0].group_id.as_str(), "readers");
        assert_eq!(decoded.groups[0].topics, None);
        assert!(decoded.groups[0].member_id.is_none());
        assert_eq!(decoded.groups[0].member_epoch, -1);
    }
}

#[test]
fn stable_queries_have_an_exact_v7_floor_and_v9_ceiling() {
    let request = group_offsets_request("readers", true);
    assert!(matches!(
        request.encoded_len(ApiVersion::new(6)),
        Err(EncodeError::FieldNotRepresentable {
            field: "RequireStable",
            ..
        })
    ));
    for version in 7..=9 {
        assert!(request.encoded_len(ApiVersion::new(version)).is_ok());
    }
    assert!(request.encoded_len(ApiVersion::new(10)).is_err());
}

fn encoded_request(require_stable: bool, version: i16) -> OffsetFetchRequest {
    let request = group_offsets_request("readers", require_stable);
    let version = ApiVersion::new(version);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("v{version} request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = OffsetFetchRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("v{version} request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
