//! Strict one-partition Fetch response correlation scenarios.

use kafka_wire::{
    FetchResponse as WireFetchResponse,
    fetch_response::{FetchableTopicResponse, PartitionData},
};
use kafka_wire_core::Uuid;

use super::{
    FetchDecodeLimits, FetchResponseFailure,
    response::{correlate_partition, normalize_correlated_response, validate_selected_version},
};

const SELECTED_VERSION: i16 = 12;

fn partition(index: i32, error_code: i16) -> PartitionData {
    let mut partition = PartitionData::default();
    partition.partition_index = index;
    partition.error_code = error_code;
    partition
}

fn topic(name: &str, partitions: Vec<PartitionData>) -> FetchableTopicResponse {
    let mut topic = FetchableTopicResponse::default();
    topic.topic = name.into();
    topic.partitions = partitions;
    topic
}

fn topic_id(id: [u8; 16], partitions: Vec<PartitionData>) -> FetchableTopicResponse {
    let mut topic = FetchableTopicResponse::default();
    topic.topic_id = Uuid::from_bytes(id);
    topic.partitions = partitions;
    topic
}

fn response(topics: Vec<FetchableTopicResponse>) -> WireFetchResponse {
    let mut response = WireFetchResponse::default();
    response.responses = topics;
    response
}

fn normalize(response: WireFetchResponse) -> Result<super::FetchResponse, FetchResponseFailure> {
    normalize_with(response, SELECTED_VERSION, FetchDecodeLimits::default())
}

fn normalize_with(
    response: WireFetchResponse,
    selected_version: i16,
    limits: FetchDecodeLimits,
) -> Result<super::FetchResponse, FetchResponseFailure> {
    validate_selected_version(selected_version)?;
    let _partition = correlate_partition("events", None, 3, selected_version, &response)?;
    normalize_correlated_response(response, limits)
}

#[test]
fn selected_version_must_preserve_name_routing_and_fetch_semantics() {
    for actual in [i16::MIN, 3, 13, i16::MAX] {
        assert_eq!(
            normalize_with(
                response(vec![topic("events", vec![partition(3, 0)])]),
                actual,
                FetchDecodeLimits::default(),
            ),
            Err(FetchResponseFailure::UnsupportedApiVersion { actual })
        );
    }
    for selected_version in [4, 12] {
        assert!(
            normalize_with(
                response(vec![topic("events", vec![partition(3, 0)])]),
                selected_version,
                FetchDecodeLimits::default(),
            )
            .is_ok()
        );
    }
    let modern = response(vec![topic_id([7; 16], vec![partition(3, 0)])]);
    validate_selected_version(16).unwrap_or_else(|error| panic!("Fetch v16: {error:?}"));
    assert!(correlate_partition("events", Some([7; 16]), 3, 16, &modern).is_ok());
}

#[test]
fn fetch_v16_correlates_by_topic_id_and_rejects_name_or_identity_substitution() {
    let modern = response(vec![topic_id([7; 16], vec![partition(3, 0)])]);
    assert!(correlate_partition("ignored-name", Some([7; 16]), 3, 16, &modern).is_ok());
    assert_eq!(
        correlate_partition("events", Some([8; 16]), 3, 16, &modern),
        Err(FetchResponseFailure::TopicIdMismatch)
    );
    assert_eq!(
        correlate_partition("events", None, 3, 16, &modern),
        Err(FetchResponseFailure::TopicIdMismatch)
    );
}

#[test]
fn correlated_result_preserves_throttle_and_exact_signed_broker_codes() {
    let mut response = response(vec![topic("events", vec![partition(3, i16::MAX)])]);
    response.throttle_time_ms = 47;
    response.error_code = -32_000;

    let normalized =
        normalize(response).unwrap_or_else(|error| panic!("correlated response: {error:?}"));
    assert_eq!(normalized.throttle_time_ms, 47);
    assert_eq!(normalized.error_code, -32_000);
    assert_eq!(normalized.topics[0].partitions[0].error_code, i16::MAX);
}

#[test]
fn missing_duplicate_and_unexpected_topics_never_correlate() {
    assert_eq!(
        normalize(response(Vec::new())),
        Err(FetchResponseFailure::TopicCount { actual: 0 })
    );
    assert_eq!(
        normalize(response(vec![
            topic("events", vec![partition(3, 0)]),
            topic("events", vec![partition(3, 0)]),
        ])),
        Err(FetchResponseFailure::TopicCount { actual: 2 })
    );
    assert_eq!(
        normalize(response(vec![topic("other", vec![partition(3, 0)])])),
        Err(FetchResponseFailure::TopicNameMismatch)
    );
}

#[test]
fn missing_duplicate_and_unexpected_partitions_never_correlate() {
    assert_eq!(
        normalize(response(vec![topic("events", Vec::new())])),
        Err(FetchResponseFailure::PartitionCount { actual: 0 })
    );
    assert_eq!(
        normalize(response(vec![topic(
            "events",
            vec![partition(3, 0), partition(3, 0)],
        )])),
        Err(FetchResponseFailure::PartitionCount { actual: 2 })
    );
    assert_eq!(
        normalize(response(vec![topic("events", vec![partition(4, 0)])])),
        Err(FetchResponseFailure::PartitionIndexMismatch { actual: 4 })
    );
    assert_eq!(
        normalize(response(vec![topic("events", vec![partition(-1, 0)])])),
        Err(FetchResponseFailure::PartitionIndexMismatch { actual: -1 })
    );
    assert_eq!(
        correlate_partition(
            "events",
            None,
            i32::MAX as u32 + 1,
            SELECTED_VERSION,
            &response(vec![topic("events", vec![partition(3, 0)])]),
        ),
        Err(FetchResponseFailure::RequestedPartitionOutOfRange {
            actual: i32::MAX as u32 + 1,
        })
    );
}

#[test]
fn correlated_shape_still_uses_the_existing_bounded_decoder() {
    let limits = FetchDecodeLimits {
        max_topics: 0,
        ..FetchDecodeLimits::default()
    };
    assert!(matches!(
        normalize_with(
            response(vec![topic("events", vec![partition(3, 0)])]),
            SELECTED_VERSION,
            limits,
        ),
        Err(FetchResponseFailure::Decode(
            super::FetchDecodeFailure::TopicCount {
                actual: 1,
                limit: 0,
            }
        ))
    ));
}
