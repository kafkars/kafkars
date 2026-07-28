//! Dynamic classic Heartbeat request construction and wire-bound scenarios.

use kafka_client_core::ClassicGeneration;
use kafka_wire::{HEARTBEAT_API_DESCRIPTOR, HeartbeatRequest};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    ClassicHeartbeatRequestFailure, classic_heartbeat_request,
    classic_heartbeat_request_with_instance,
    validation::{MAX_KAFKA_STRING_BYTES, STATIC_HEARTBEAT_VERSION},
};

fn generation(value: i32) -> ClassicGeneration {
    ClassicGeneration::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative generation"))
}

#[test]
fn static_request_carries_instance_and_round_trips_at_v3() {
    let prepared = classic_heartbeat_request_with_instance(
        "workers",
        "member-a",
        Some("instance-a"),
        generation(17),
    )
    .unwrap_or_else(|error| panic!("static Heartbeat request: {error:?}"));
    let request = prepared.request_for_test();
    assert_eq!(
        request
            .group_instance_id
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("instance-a")
    );
    let version = ApiVersion::new(STATIC_HEARTBEAT_VERSION);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("encode: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("decoder: {error}"));
    assert_eq!(
        HeartbeatRequest::decode(&mut decoder, version)
            .unwrap_or_else(|error| panic!("decode: {error}")),
        *request
    );
}

#[test]
fn request_preserves_the_exact_dynamic_member_facts() {
    let prepared = classic_heartbeat_request("workers", "member-a", generation(17))
        .unwrap_or_else(|error| panic!("Heartbeat request failed: {error:?}"));
    let request = prepared.request_for_test();

    assert_eq!(request.group_id.as_str(), "workers");
    assert_eq!(request.generation_id, 17);
    assert_eq!(request.member_id.as_str(), "member-a");
    assert_eq!(request.group_instance_id, None);
    assert!(request.unknown_tagged_fields.is_empty());
}

#[test]
fn generated_request_round_trips_at_both_exact_driver_bounds() {
    let prepared = classic_heartbeat_request("workers", "member-a", generation(17))
        .unwrap_or_else(|error| panic!("Heartbeat request failed: {error:?}"));
    let request = prepared.request_for_test();

    for version in [ApiVersion::new(0), ApiVersion::new(2)] {
        assert!(
            HEARTBEAT_API_DESCRIPTOR
                .supported_versions
                .contains(version)
        );
        let mut encoded = BytesMut::new();
        request
            .encode_into(&mut encoded, version)
            .unwrap_or_else(|error| panic!("Heartbeat encode failed: {error}"));
        let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
            .unwrap_or_else(|error| panic!("decoder failed: {error}"));
        let decoded = HeartbeatRequest::decode(&mut decoder, version)
            .unwrap_or_else(|error| panic!("Heartbeat decode failed: {error}"));
        decoder
            .finish()
            .unwrap_or_else(|error| panic!("Heartbeat trailing bytes: {error}"));
        assert_eq!(&decoded, request);
    }
}

#[test]
fn group_and_member_bounds_are_exact() {
    let maximum = "x".repeat(MAX_KAFKA_STRING_BYTES);
    assert!(
        classic_heartbeat_request(&maximum, &maximum, generation(0)).is_ok(),
        "exact Kafka string bounds should be accepted"
    );

    let oversized = "x".repeat(MAX_KAFKA_STRING_BYTES + 1);
    assert_eq!(
        classic_heartbeat_request(&oversized, "member-a", generation(0)).err(),
        Some(ClassicHeartbeatRequestFailure::GroupName)
    );
    assert_eq!(
        classic_heartbeat_request("workers", &oversized, generation(0)).err(),
        Some(ClassicHeartbeatRequestFailure::MemberId)
    );
    assert_eq!(
        classic_heartbeat_request("", "member-a", generation(0)).err(),
        Some(ClassicHeartbeatRequestFailure::GroupName)
    );
    assert_eq!(
        classic_heartbeat_request("workers", "", generation(0)).err(),
        Some(ClassicHeartbeatRequestFailure::MemberId)
    );
}

#[test]
fn prepared_owner_transfers_the_generated_request_without_reconstruction() {
    let prepared = classic_heartbeat_request("workers", "member-a", generation(i32::MAX))
        .unwrap_or_else(|error| panic!("Heartbeat request failed: {error:?}"));
    let request = prepared.into_generated_heartbeat_request();

    assert_eq!(request.group_id.as_str(), "workers");
    assert_eq!(request.generation_id, i32::MAX);
    assert_eq!(request.member_id.as_str(), "member-a");
}
