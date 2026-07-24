//! Strict one-partition Fetch response correlation scenarios.

use kafka_wire::{
    FetchResponse as WireFetchResponse,
    fetch_response::{FetchableTopicResponse, PartitionData},
};

use super::{FetchDecodeLimits, FetchResponseFailure, normalize_one_partition_fetch_response};

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

fn response(topics: Vec<FetchableTopicResponse>) -> WireFetchResponse {
    let mut response = WireFetchResponse::default();
    response.responses = topics;
    response
}

fn normalize(response: WireFetchResponse) -> Result<super::FetchResponse, FetchResponseFailure> {
    normalize_one_partition_fetch_response("events", 3, response, FetchDecodeLimits::default())
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
        normalize_one_partition_fetch_response(
            "events",
            i32::MAX as u32 + 1,
            response(vec![topic("events", vec![partition(3, 0)])]),
            FetchDecodeLimits::default(),
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
        normalize_one_partition_fetch_response(
            "events",
            3,
            response(vec![topic("events", vec![partition(3, 0)])]),
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
