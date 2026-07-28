//! Dynamic Range Join request construction and inner-metadata scenarios.

use std::sync::Arc;

use kafka_client_core::{
    CLASSIC_GROUP_TIMEOUT_MAX_MS, CLASSIC_GROUP_TIMEOUT_MIN_MS, ClassicGroupTiming,
};
use kafka_wire::{
    JOIN_GROUP_API_DESCRIPTOR, JoinGroupRequest, decode_consumer_protocol_subscription,
};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    ClassicJoinRequestFailure, classic_join_group_request,
    classic_join_group_request_with_instance,
    validation::{INNER_SCHEMA_VERSION, MAX_TOPICS, RANGE_PROTOCOL, STATIC_JOIN_VERSION},
};

fn topics(values: &[&str]) -> Vec<Arc<str>> {
    values.iter().copied().map(Arc::from).collect()
}

#[test]
fn static_request_carries_instance_and_round_trips_at_v5() {
    let prepared = classic_join_group_request_with_instance(
        "workers",
        Some("member-a"),
        Some("instance-a"),
        &topics(&["orders"]),
        timing(),
    )
    .unwrap_or_else(|error| panic!("static Join: {error:?}"));
    let request = prepared.request_for_test();
    assert_eq!(
        request
            .group_instance_id
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("instance-a")
    );
    let version = ApiVersion::new(STATIC_JOIN_VERSION);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("encode: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("decoder: {error}"));
    assert_eq!(
        JoinGroupRequest::decode(&mut decoder, version)
            .unwrap_or_else(|error| panic!("decode: {error}")),
        *request
    );
}

fn timing() -> ClassicGroupTiming {
    ClassicGroupTiming::try_new(10_000, 30_000)
        .unwrap_or_else(|error| panic!("timing failed: {error}"))
}

#[test]
fn request_is_dynamic_range_with_one_v0_subscription() {
    let prepared =
        classic_join_group_request("workers", None, &topics(&["orders", "payments"]), timing())
            .unwrap_or_else(|error| panic!("Join request failed: {error:?}"));
    let request = prepared.request_for_test();

    assert_eq!(request.group_id.as_str(), "workers");
    assert_eq!(request.member_id.as_str(), "");
    assert_eq!(request.session_timeout_ms, 10_000);
    assert_eq!(request.rebalance_timeout_ms, 30_000);
    assert_eq!(request.protocol_type.as_str(), "consumer");
    assert_eq!(request.group_instance_id, None);
    assert_eq!(request.reason, None);
    assert_eq!(request.protocols.len(), 1);
    assert_eq!(request.protocols[0].name.as_str(), RANGE_PROTOCOL);
    let (version, subscription) = decode_consumer_protocol_subscription(
        request.protocols[0].metadata.clone(),
        DecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("subscription decode failed: {error}"));
    assert_eq!(version.value(), INNER_SCHEMA_VERSION);
    assert_eq!(
        subscription
            .topics
            .iter()
            .map(kafka_wire_core::StrBytes::as_str)
            .collect::<Vec<_>>(),
        ["orders", "payments"]
    );
    assert_eq!(subscription.user_data, None);
    assert!(subscription.owned_partitions.is_empty());
    assert_eq!(subscription.generation_id, -1);
    assert_eq!(subscription.rack_id, None);
}

#[test]
fn generated_request_round_trips_at_both_exact_driver_bounds() {
    let prepared =
        classic_join_group_request("workers", Some("member-a"), &topics(&["orders"]), timing())
            .unwrap_or_else(|error| panic!("Join request failed: {error:?}"));
    let request = prepared.request_for_test();

    for version in [ApiVersion::new(1), ApiVersion::new(3)] {
        assert!(
            JOIN_GROUP_API_DESCRIPTOR
                .supported_versions
                .contains(version)
        );
        let mut encoded = BytesMut::new();
        request
            .encode_into(&mut encoded, version)
            .unwrap_or_else(|error| panic!("Join encode failed: {error}"));
        let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
            .unwrap_or_else(|error| panic!("decoder failed: {error}"));
        let decoded = JoinGroupRequest::decode(&mut decoder, version)
            .unwrap_or_else(|error| panic!("Join decode failed: {error}"));
        decoder
            .finish()
            .unwrap_or_else(|error| panic!("Join trailing bytes: {error}"));
        assert_eq!(&decoded, request);
    }
}

#[test]
fn exact_timing_bounds_round_trip_unchanged() {
    let timing =
        ClassicGroupTiming::try_new(CLASSIC_GROUP_TIMEOUT_MIN_MS, CLASSIC_GROUP_TIMEOUT_MAX_MS)
            .unwrap_or_else(|error| panic!("timing failed: {error}"));
    let prepared = classic_join_group_request("workers", None, &topics(&["orders"]), timing)
        .unwrap_or_else(|error| panic!("Join request failed: {error:?}"));
    let request = prepared.request_for_test();
    let version = ApiVersion::new(3);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("Join encode failed: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("decoder failed: {error}"));
    let decoded = JoinGroupRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("Join decode failed: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("Join trailing bytes: {error}"));

    assert_eq!(decoded.session_timeout_ms, 1);
    assert_eq!(decoded.rebalance_timeout_ms, i32::MAX);
}

#[test]
fn structural_rejection_precedes_generated_request_ownership() {
    let duplicate = topics(&["orders", "orders"]);
    assert_eq!(
        classic_join_group_request("workers", None, &duplicate, timing()).err(),
        Some(ClassicJoinRequestFailure::DuplicateTopic)
    );
    let unordered = topics(&["payments", "orders"]);
    assert_eq!(
        classic_join_group_request("workers", None, &unordered, timing()).err(),
        Some(ClassicJoinRequestFailure::OutOfOrderTopic)
    );
    assert_eq!(
        classic_join_group_request("workers", Some(""), &[], timing()).err(),
        Some(ClassicJoinRequestFailure::InvalidMember)
    );
}

#[test]
fn topic_count_and_spelling_bounds_are_exact() {
    let maximum = (0..MAX_TOPICS)
        .map(|index| Arc::<str>::from(format!("topic-{index:02}")))
        .collect::<Vec<_>>();
    assert!(
        classic_join_group_request("workers", None, &maximum, timing()).is_ok(),
        "the exact topic bound should be accepted"
    );
    let mut oversized = maximum;
    oversized.push(Arc::from("topic-64"));
    assert_eq!(
        classic_join_group_request("workers", None, &oversized, timing()).err(),
        Some(ClassicJoinRequestFailure::TopicCount {
            actual: MAX_TOPICS + 1,
            limit: MAX_TOPICS,
        })
    );
    assert_eq!(
        classic_join_group_request("workers", None, &[Arc::from("x".repeat(250))], timing(),).err(),
        Some(ClassicJoinRequestFailure::InvalidTopic)
    );
}
