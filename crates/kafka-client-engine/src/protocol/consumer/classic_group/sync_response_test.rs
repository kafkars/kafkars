//! Sync response version, assignment-shape, and broker-error scenarios.

use std::sync::Arc;

use kafka_wire::{
    ConsumerProtocolAssignment, SyncGroupResponse, consumer_protocol_assignment::TopicPartition,
    encode_consumer_protocol_assignment,
};
use kafka_wire_core::{ApiVersion, BytesMut};

use super::{
    ClassicSyncOutcome, ClassicSyncResponseFailure, normalize_classic_sync_response,
    validation::MAX_MEMBER_PARTITIONS,
};

fn topic(name: &str, partitions: &[i32]) -> TopicPartition {
    let mut topic = TopicPartition::default();
    topic.topic = name.into();
    topic.partitions = partitions.to_vec();
    topic
}

fn payload(version: i16, topics: Vec<TopicPartition>) -> kafka_wire_core::Bytes {
    let mut assignment = ConsumerProtocolAssignment::default();
    assignment.assigned_partitions = topics;
    assignment.user_data = None;
    let mut encoded = BytesMut::new();
    encode_consumer_protocol_assignment(&mut encoded, &assignment, ApiVersion::new(version))
        .unwrap_or_else(|error| panic!("assignment encode failed: {error}"));
    encoded.freeze()
}

fn response(topics: Vec<TopicPartition>) -> SyncGroupResponse {
    let mut response = SyncGroupResponse::default();
    response.assignment = payload(0, topics);
    response
}

#[test]
fn success_returns_ordered_arc_shared_named_partitions() {
    let raw = response(vec![topic("orders", &[0, 1]), topic("payments", &[2])]);
    let normalized = normalize_classic_sync_response(2, &raw)
        .unwrap_or_else(|error| panic!("Sync normalization failed: {error:?}"));
    let ClassicSyncOutcome::Assigned {
        throttle_time_ms,
        partitions,
    } = normalized
    else {
        panic!("assigned outcome expected");
    };
    assert_eq!(throttle_time_ms, 0);
    assert_eq!(partitions.len(), 3);
    assert_eq!(partitions[0].topic(), "orders");
    assert_eq!(partitions[0].partition(), 0);
    let mut partitions = partitions.into_iter();
    let (first_topic, _) = partitions
        .next()
        .unwrap_or_else(|| panic!("first partition"))
        .into_parts();
    let (second_topic, _) = partitions
        .next()
        .unwrap_or_else(|| panic!("second partition"))
        .into_parts();
    assert_eq!(first_topic, Arc::<str>::from("orders"));
    assert!(Arc::ptr_eq(&first_topic, &second_topic));
}

#[test]
fn exact_versions_throttles_and_signed_broker_codes_are_preserved() {
    for version in [-1, 3] {
        assert_eq!(
            normalize_classic_sync_response(version, &response(Vec::new())),
            Err(ClassicSyncResponseFailure::UnsupportedApiVersion(version))
        );
    }
    let mut impossible = response(Vec::new());
    impossible.throttle_time_ms = 1;
    assert_eq!(
        normalize_classic_sync_response(0, &impossible),
        Err(ClassicSyncResponseFailure::UnexpectedThrottleTime(1))
    );
    impossible.throttle_time_ms = -1;
    assert_eq!(
        normalize_classic_sync_response(2, &impossible),
        Err(ClassicSyncResponseFailure::NegativeThrottleTime(-1))
    );
    let mut rejected = response(Vec::new());
    rejected.error_code = -321;
    rejected.throttle_time_ms = 8;
    rejected.assignment = kafka_wire_core::Bytes::from_static(b"not an assignment");
    let normalized = normalize_classic_sync_response(2, &rejected)
        .unwrap_or_else(|error| panic!("broker rejection failed: {error:?}"));
    let ClassicSyncOutcome::Rejected(rejection) = normalized else {
        panic!("broker rejection expected");
    };
    assert_eq!(rejection.error_code().get(), -321);
    assert_eq!(rejection.throttle_time_ms(), 8);
}

#[test]
fn optional_protocol_fields_are_not_inferred_in_the_v0_v2_window() {
    let mut raw = response(Vec::new());
    raw.protocol_type = Some("consumer".into());
    assert_eq!(
        normalize_classic_sync_response(2, &raw),
        Err(ClassicSyncResponseFailure::UnexpectedProtocolType)
    );
    raw.protocol_type = None;
    raw.protocol_name = Some("range".into());
    assert_eq!(
        normalize_classic_sync_response(2, &raw),
        Err(ClassicSyncResponseFailure::UnexpectedProtocolName)
    );
}

#[test]
fn inner_assignment_version_and_partition_count_are_bounded() {
    let mut raw = response(Vec::new());
    raw.assignment = payload(1, Vec::new());
    assert_eq!(
        normalize_classic_sync_response(2, &raw),
        Err(ClassicSyncResponseFailure::UnsupportedAssignmentVersion(1))
    );
    raw.assignment = payload(
        0,
        vec![
            topic("orders", &(0..32).collect::<Vec<_>>()),
            topic("payments", &(0..=32).collect::<Vec<_>>()),
        ],
    );
    assert_eq!(
        normalize_classic_sync_response(2, &raw),
        Err(ClassicSyncResponseFailure::PartitionCount {
            actual: MAX_MEMBER_PARTITIONS + 1,
            limit: MAX_MEMBER_PARTITIONS,
        })
    );
    let mut assignment = ConsumerProtocolAssignment::default();
    assignment.user_data = Some(kafka_wire_core::Bytes::from_static(b"opaque"));
    let mut encoded = BytesMut::new();
    encode_consumer_protocol_assignment(&mut encoded, &assignment, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("assignment encode failed: {error}"));
    raw.assignment = encoded.freeze();
    assert_eq!(
        normalize_classic_sync_response(2, &raw),
        Err(ClassicSyncResponseFailure::AssignmentUserData)
    );
}

#[test]
fn wire_order_is_preserved_for_later_catalog_canonicalization() {
    let ClassicSyncOutcome::Assigned { partitions, .. } = normalize_classic_sync_response(
        2,
        &response(vec![topic("zeta", &[2, 0]), topic("alpha", &[3, 1])]),
    )
    .unwrap_or_else(|error| panic!("unsorted assignment failed: {error:?}")) else {
        panic!("assigned outcome expected");
    };
    assert_eq!(
        partitions
            .iter()
            .map(|partition| (partition.topic(), partition.partition()))
            .collect::<Vec<_>>(),
        [("zeta", 2), ("zeta", 0), ("alpha", 3), ("alpha", 1)]
    );
}

#[test]
fn empty_topic_entries_still_participate_in_duplicate_checks() {
    assert_eq!(
        normalize_classic_sync_response(
            2,
            &response(vec![topic("orders", &[]), topic("orders", &[])])
        ),
        Err(ClassicSyncResponseFailure::DuplicateTopic)
    );
}

#[test]
fn partition_sentinels_and_duplicates_are_rejected() {
    assert_eq!(
        normalize_classic_sync_response(2, &response(vec![topic("orders", &[-1])])),
        Err(ClassicSyncResponseFailure::NegativePartition(-1))
    );
    assert_eq!(
        normalize_classic_sync_response(2, &response(vec![topic("orders", &[1, 1])])),
        Err(ClassicSyncResponseFailure::DuplicatePartition(1))
    );
}
