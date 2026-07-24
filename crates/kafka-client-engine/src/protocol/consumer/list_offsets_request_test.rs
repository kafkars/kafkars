//! One-partition `ListOffsets` request construction scenarios.

use kafka_client_core::{NextFetchOffset, PartitionIndex, StartPosition};

use super::{ListOffsetsIsolation, ListOffsetsRequestFailure, list_offsets_request};

fn partition(value: u32) -> PartitionIndex {
    PartitionIndex::from_raw(value)
}

#[test]
fn beginning_and_end_use_kafka_sentinels_and_normal_consumer_fields() {
    let beginning = list_offsets_request(
        "audit-log",
        partition(7),
        StartPosition::Beginning,
        ListOffsetsIsolation::ReadUncommitted,
        4_321,
    )
    .unwrap_or_else(|error| panic!("valid beginning query: {error:?}"));
    assert_eq!(beginning.replica_id, -1);
    assert_eq!(beginning.isolation_level, 0);
    assert_eq!(beginning.timeout_ms, 4_321);
    assert_eq!(beginning.topics.len(), 1);
    assert_eq!(beginning.topics[0].name.as_str(), "audit-log");
    assert_eq!(beginning.topics[0].partitions.len(), 1);
    let beginning_partition = &beginning.topics[0].partitions[0];
    assert_eq!(beginning_partition.partition_index, 7);
    assert_eq!(beginning_partition.current_leader_epoch, -1);
    assert_eq!(beginning_partition.timestamp, -2);

    let end = list_offsets_request(
        "audit-log",
        partition(7),
        StartPosition::End,
        ListOffsetsIsolation::ReadCommitted,
        0,
    )
    .unwrap_or_else(|error| panic!("valid end query: {error:?}"));
    assert_eq!(end.isolation_level, 1);
    assert_eq!(end.topics[0].partitions[0].timestamp, -1);
}

#[test]
fn invalid_catalog_and_effect_facts_never_reach_generated_storage() {
    let maximum = "t".repeat(249);
    assert!(
        list_offsets_request(
            &maximum,
            partition(0),
            StartPosition::Beginning,
            ListOffsetsIsolation::ReadUncommitted,
            0,
        )
        .is_ok()
    );
    assert_eq!(
        list_offsets_request(
            "",
            partition(0),
            StartPosition::Beginning,
            ListOffsetsIsolation::ReadUncommitted,
            0,
        ),
        Err(ListOffsetsRequestFailure::EmptyTopic)
    );
    let overlong = "t".repeat(250);
    assert_eq!(
        list_offsets_request(
            &overlong,
            partition(0),
            StartPosition::Beginning,
            ListOffsetsIsolation::ReadUncommitted,
            0,
        ),
        Err(ListOffsetsRequestFailure::TopicTooLong {
            actual: 250,
            limit: 249,
        })
    );
    assert_eq!(
        list_offsets_request(
            "audit",
            partition(i32::MAX as u32 + 1),
            StartPosition::Beginning,
            ListOffsetsIsolation::ReadUncommitted,
            0,
        ),
        Err(ListOffsetsRequestFailure::PartitionOutOfRange {
            actual: i32::MAX as u32 + 1,
        })
    );
    let offset =
        NextFetchOffset::try_from_raw(9).unwrap_or_else(|| panic!("nonnegative test offset"));
    assert_eq!(
        list_offsets_request(
            "audit",
            partition(0),
            StartPosition::Offset(offset),
            ListOffsetsIsolation::ReadUncommitted,
            0,
        ),
        Err(ListOffsetsRequestFailure::ExplicitOffset)
    );
    assert_eq!(
        list_offsets_request(
            "audit",
            partition(0),
            StartPosition::End,
            ListOffsetsIsolation::ReadUncommitted,
            -1,
        ),
        Err(ListOffsetsRequestFailure::NegativeTimeout { actual: -1 })
    );
}
