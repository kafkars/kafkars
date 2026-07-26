//! Successful legacy and modern normalization in exact caller assignment order.

use std::sync::Arc;

use core::num::NonZeroI16;

use kafka_wire::{
    OffsetFetchResponse,
    offset_fetch_response::{
        OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
        OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
};

use super::{
    model::{GroupOffsetFetchCorrelation, GroupOffsetFetchPartitionValueRef},
    preparation::{GroupOffsetFetchPreparation, prepare_group_offset_fetch_request},
    preparation_test::topic,
    response::normalize_group_offset_fetch_response,
};

#[test]
fn legacy_results_restore_caller_order_and_preserve_exact_values() {
    let mut response = OffsetFetchResponse::default();
    response.throttle_time_ms = 12;
    response.topics = vec![
        legacy_topic("a", vec![legacy_partition(1, 42, 5, Some(""), 0)]),
        legacy_topic(
            "z",
            vec![
                legacy_partition(0, -77, -88, Some("ignored"), -731),
                legacy_partition(2, -1, -1, None, 0),
            ],
        ),
    ];

    let normalized =
        normalize_group_offset_fetch_response(&correlation(), &response, 7, usize::MAX)
            .unwrap_or_else(|error| panic!("valid legacy response: {error:?}"));
    assert_eq!(normalized.throttle_time_ms(), 12);
    assert_eq!(normalized.top_level_error(), None);
    assert_eq!(
        normalized.entries(),
        [
            GroupOffsetFetchPartitionValueRef::Fetched {
                committed_offset: None,
                committed_leader_epoch: None,
                metadata: None,
            },
            GroupOffsetFetchPartitionValueRef::Rejected {
                code: NonZeroI16::new(-731).unwrap_or_else(|| panic!("nonzero")),
            },
            GroupOffsetFetchPartitionValueRef::Fetched {
                committed_offset: Some(42),
                committed_leader_epoch: Some(5),
                metadata: Some(""),
            },
        ]
    );
}

#[test]
fn modern_results_require_and_normalize_the_matching_group() {
    let response = modern_response(
        "readers",
        0,
        vec![
            modern_topic("a", vec![modern_partition(1, 42, 5, Some("m"), 0)]),
            modern_topic(
                "z",
                vec![
                    modern_partition(0, 11, -1, None, 0),
                    modern_partition(2, 7, 2, None, 0),
                ],
            ),
        ],
    );

    let normalized =
        normalize_group_offset_fetch_response(&correlation(), &response, 9, usize::MAX)
            .unwrap_or_else(|error| panic!("valid modern response: {error:?}"));
    assert_eq!(normalized.entries().len(), 3);
    assert!(matches!(
        normalized.entries()[0],
        GroupOffsetFetchPartitionValueRef::Fetched {
            committed_offset: Some(7),
            committed_leader_epoch: Some(2),
            ..
        }
    ));
    assert!(matches!(
        normalized.entries()[2],
        GroupOffsetFetchPartitionValueRef::Fetched {
            committed_offset: Some(42),
            metadata: Some("m"),
            ..
        }
    ));
}

#[test]
fn exact_signed_legacy_and_modern_group_errors_remain_top_level() {
    let mut legacy = OffsetFetchResponse::default();
    legacy.error_code = -911;
    let normalized = normalize_group_offset_fetch_response(&correlation(), &legacy, 7, usize::MAX)
        .unwrap_or_else(|error| panic!("legacy group error: {error:?}"));
    assert_eq!(
        normalized.top_level_error().map(NonZeroI16::get),
        Some(-911)
    );
    assert!(normalized.entries().is_empty());

    let modern = modern_response("readers", -977, Vec::new());
    let normalized = normalize_group_offset_fetch_response(&correlation(), &modern, 9, usize::MAX)
        .unwrap_or_else(|error| panic!("modern group error: {error:?}"));
    assert_eq!(
        normalized.top_level_error().map(NonZeroI16::get),
        Some(-977)
    );
}

#[test]
fn full_normalized_charge_is_proven_before_result_binding() {
    let mut response = OffsetFetchResponse::default();
    response.topics = vec![
        legacy_topic(
            "z",
            vec![
                legacy_partition(2, 2, -1, Some("metadata"), 0),
                legacy_partition(0, 0, -1, None, 0),
            ],
        ),
        legacy_topic("a", vec![legacy_partition(1, 1, -1, None, 0)]),
    ];
    let normalized =
        normalize_group_offset_fetch_response(&correlation(), &response, 7, usize::MAX)
            .unwrap_or_else(|error| panic!("charge can be measured: {error:?}"));
    assert!(
        normalize_group_offset_fetch_response(
            &correlation(),
            &response,
            7,
            normalized.retained_charge() - 1,
        )
        .is_err()
    );
}

pub(super) fn correlation() -> GroupOffsetFetchCorrelation {
    let GroupOffsetFetchPreparation::Prepared(prepared) = prepare_group_offset_fetch_request(
        Arc::from("readers"),
        vec![topic("z", &[2, 0]), topic("a", &[1])],
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("valid fixture assignment: {error:?}")) else {
        panic!("fixture assignment is nonempty");
    };
    prepared.into_parts().0
}

pub(super) fn legacy_topic(
    name: &str,
    partitions: Vec<OffsetFetchResponsePartition>,
) -> OffsetFetchResponseTopic {
    let mut topic = OffsetFetchResponseTopic::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

pub(super) fn legacy_partition(
    index: i32,
    offset: i64,
    epoch: i32,
    metadata: Option<&str>,
    error_code: i16,
) -> OffsetFetchResponsePartition {
    let mut partition = OffsetFetchResponsePartition::default();
    partition.partition_index = index;
    partition.committed_offset = offset;
    partition.committed_leader_epoch = epoch;
    partition.metadata = metadata.map(Into::into);
    partition.error_code = error_code;
    partition
}

pub(super) fn modern_response(
    group_id: &str,
    error_code: i16,
    topics: Vec<OffsetFetchResponseTopics>,
) -> OffsetFetchResponse {
    let mut group = OffsetFetchResponseGroup::default();
    group.group_id = group_id.into();
    group.error_code = error_code;
    group.topics = topics;
    let mut response = OffsetFetchResponse::default();
    response.groups = vec![group];
    response
}

pub(super) fn modern_topic(
    name: &str,
    partitions: Vec<OffsetFetchResponsePartitions>,
) -> OffsetFetchResponseTopics {
    let mut topic = OffsetFetchResponseTopics::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

pub(super) fn modern_partition(
    index: i32,
    offset: i64,
    epoch: i32,
    metadata: Option<&str>,
    error_code: i16,
) -> OffsetFetchResponsePartitions {
    let mut partition = OffsetFetchResponsePartitions::default();
    partition.partition_index = index;
    partition.committed_offset = offset;
    partition.committed_leader_epoch = epoch;
    partition.metadata = metadata.map(Into::into);
    partition.error_code = error_code;
    partition
}
