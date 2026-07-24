//! One-partition name-based Fetch request construction scenarios.

use kafka_wire_core::{ApiVersion, KafkaEncode};

use super::{
    FETCH_NAME_ROUTE_MAX_VERSION, FETCH_NAME_ROUTE_MIN_VERSION, FetchRequestFailure,
    FetchRequestSettings, fetch_request,
};

fn settings(isolation_level: i8) -> FetchRequestSettings {
    FetchRequestSettings::new(500, 1, 50 * 1024 * 1024, 1024 * 1024, isolation_level)
}

#[test]
fn request_uses_generated_v12_name_and_exact_consumer_sentinels() {
    let request = fetch_request("events", 7, 42, settings(1))
        .unwrap_or_else(|error| panic!("valid Fetch request: {error:?}"));

    assert_eq!(
        (FETCH_NAME_ROUTE_MIN_VERSION, FETCH_NAME_ROUTE_MAX_VERSION),
        (4, 12)
    );
    assert_eq!(request.replica_id, -1);
    assert_eq!(request.max_wait_ms, 500);
    assert_eq!(request.min_bytes, 1);
    assert_eq!(request.max_bytes, 50 * 1024 * 1024);
    assert_eq!(request.isolation_level, 1);
    assert_eq!(request.session_id, 0);
    assert_eq!(request.session_epoch, -1);
    assert!(request.cluster_id.is_none());
    assert!(request.forgotten_topics_data.is_empty());
    assert!(request.rack_id.as_str().is_empty());
    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].topic.as_str(), "events");
    assert_eq!(request.topics[0].partitions.len(), 1);
    let partition = &request.topics[0].partitions[0];
    assert_eq!(partition.partition, 7);
    assert_eq!(partition.current_leader_epoch, -1);
    assert_eq!(partition.fetch_offset, 42);
    assert_eq!(partition.last_fetched_epoch, -1);
    assert_eq!(partition.log_start_offset, -1);
    assert_eq!(partition.partition_max_bytes, 1024 * 1024);
    assert!(
        request
            .encoded_len(ApiVersion::new(FETCH_NAME_ROUTE_MAX_VERSION))
            .is_ok()
    );
    assert!(
        request
            .encoded_len(ApiVersion::new(FETCH_NAME_ROUTE_MIN_VERSION))
            .is_ok()
    );
}

#[test]
fn both_kafka_isolation_levels_are_represented_exactly() {
    for isolation_level in [0, 1] {
        let request = fetch_request("events", 0, 0, settings(isolation_level))
            .unwrap_or_else(|error| panic!("valid isolation level: {error:?}"));
        assert_eq!(request.isolation_level, isolation_level);
    }
    assert_eq!(
        fetch_request("events", 0, 0, settings(2)),
        Err(FetchRequestFailure::InvalidIsolationLevel { actual: 2 })
    );
}

#[test]
fn topic_partition_and_offset_are_validated_before_generated_storage() {
    assert_eq!(
        fetch_request("", 0, 0, settings(0)),
        Err(FetchRequestFailure::EmptyTopic)
    );
    let long_topic = "t".repeat(250);
    assert_eq!(
        fetch_request(&long_topic, 0, 0, settings(0)),
        Err(FetchRequestFailure::TopicTooLong {
            actual: 250,
            limit: 249,
        })
    );
    assert_eq!(
        fetch_request("events", i32::MAX as u32 + 1, 0, settings(0)),
        Err(FetchRequestFailure::PartitionOutOfRange {
            actual: i32::MAX as u32 + 1,
        })
    );
    assert_eq!(
        fetch_request("events", 0, -1, settings(0)),
        Err(FetchRequestFailure::NegativeFetchOffset { actual: -1 })
    );
}

#[test]
fn request_and_partition_byte_bounds_reject_zero_overflow_and_inversion() {
    let maximum = i32::MAX as u32;
    assert!(
        fetch_request(
            "events",
            0,
            0,
            FetchRequestSettings::new(maximum, maximum, maximum, maximum, 0),
        )
        .is_ok()
    );

    for (settings, expected) in [
        (
            FetchRequestSettings::new(maximum + 1, 0, 1, 1, 0),
            FetchRequestFailure::MaxWaitOutOfRange {
                actual: maximum + 1,
            },
        ),
        (
            FetchRequestSettings::new(0, maximum + 1, maximum + 1, 1, 0),
            FetchRequestFailure::MinBytesOutOfRange {
                actual: maximum + 1,
            },
        ),
        (
            FetchRequestSettings::new(0, 0, 0, 1, 0),
            FetchRequestFailure::MaxBytesOutOfRange { actual: 0 },
        ),
        (
            FetchRequestSettings::new(0, 0, maximum + 1, 1, 0),
            FetchRequestFailure::MaxBytesOutOfRange {
                actual: maximum + 1,
            },
        ),
        (
            FetchRequestSettings::new(0, 0, 1, 0, 0),
            FetchRequestFailure::PartitionMaxBytesOutOfRange { actual: 0 },
        ),
        (
            FetchRequestSettings::new(0, 0, 1, maximum + 1, 0),
            FetchRequestFailure::PartitionMaxBytesOutOfRange {
                actual: maximum + 1,
            },
        ),
        (
            FetchRequestSettings::new(0, 2, 1, 1, 0),
            FetchRequestFailure::MinBytesExceedMaxBytes {
                min_bytes: 2,
                max_bytes: 1,
            },
        ),
    ] {
        assert_eq!(fetch_request("events", 0, 0, settings), Err(expected));
    }
}
