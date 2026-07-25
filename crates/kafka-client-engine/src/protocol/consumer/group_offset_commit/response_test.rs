//! Exact response correlation, ordering, and bounded result normalization.

use std::sync::Arc;

use kafka_client_core::GroupOffsetCommitPartitionResult;
use kafka_wire::{
    OffsetCommitResponse,
    offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
};

use super::{
    model_test::{entry, prepared, topic},
    response::{GroupOffsetCommitProtocolFailure, normalize_group_offset_commit_response},
    validation::MAX_GROUP_OFFSET_COMMIT_ENTRIES,
};

#[test]
fn response_restores_checkpoint_order_and_exact_signed_codes_without_diagnostics() {
    let prepared = prepared(
        vec![
            entry(1, 0, 10, None),
            entry(1, 2, 30, None),
            entry(2, 1, 20, None),
        ],
        4,
        vec![topic(1, Arc::from("orders")), topic(2, Arc::from("audit"))],
    );
    let outcome_capacity = prepared.outcomes_capacity();
    let outcome_pointer = prepared.outcomes_ptr_for_test();
    let response = response(
        17,
        vec![
            response_topic("audit", vec![response_partition(1, -32_123)]),
            response_topic(
                "orders",
                vec![response_partition(2, 0), response_partition(0, 7)],
            ),
        ],
    );
    let (throttle, outcomes) = normalize_group_offset_commit_response(prepared, &response)
        .unwrap_or_else(|error| panic!("correlatable response: {error:?}"));
    assert_eq!(throttle, 17);
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| (outcome.topic_id().get(), outcome.partition().get()))
            .collect::<Vec<_>>(),
        vec![(1, 0), (1, 2), (2, 1)]
    );
    let GroupOffsetCommitPartitionResult::Rejected(first_error) = outcomes[0].result() else {
        panic!("first partition must preserve broker rejection");
    };
    assert_eq!(first_error.code(), 7);
    assert!(matches!(
        outcomes[1].result(),
        GroupOffsetCommitPartitionResult::Committed
    ));
    let GroupOffsetCommitPartitionResult::Rejected(last_error) = outcomes[2].result() else {
        panic!("last partition must preserve signed broker rejection");
    };
    assert_eq!(last_error.code(), -32_123);
    assert_eq!(outcomes.capacity(), outcome_capacity);
    assert_eq!(outcomes.as_ptr(), outcome_pointer);
}

#[test]
fn negative_throttle_and_wrong_topic_partition_shapes_are_invalid() {
    let cases = [
        (
            response(
                -1,
                vec![
                    response_topic("orders", vec![response_partition(0, 0)]),
                    response_topic("audit", vec![response_partition(1, 0)]),
                ],
            ),
            GroupOffsetCommitProtocolFailure::ThrottleTime,
        ),
        (
            response(
                0,
                vec![
                    response_topic("orders", vec![response_partition(0, 0)]),
                    response_topic("payments", vec![response_partition(1, 0)]),
                ],
            ),
            GroupOffsetCommitProtocolFailure::UnexpectedTopic,
        ),
        (
            response(
                0,
                vec![
                    response_topic("orders", vec![response_partition(0, 0)]),
                    response_topic("audit", vec![response_partition(2, 0)]),
                ],
            ),
            GroupOffsetCommitProtocolFailure::UnexpectedPartition,
        ),
        (
            response(
                0,
                vec![
                    response_topic("orders", vec![response_partition(0, 0)]),
                    response_topic("orders", vec![response_partition(0, 0)]),
                ],
            ),
            GroupOffsetCommitProtocolFailure::DuplicateTopic,
        ),
    ];
    for (response, expected) in cases {
        assert_eq!(
            normalize_group_offset_commit_response(two_partition_prepared(), &response),
            Err(expected)
        );
    }
}

#[test]
fn malformed_response_counts_are_invalid() {
    assert_eq!(
        normalize_group_offset_commit_response(one_partition_prepared(), &response(0, vec![])),
        Err(GroupOffsetCommitProtocolFailure::ResultCount)
    );

    let oversized = response_topic(
        "orders",
        (0..=i32::try_from(MAX_GROUP_OFFSET_COMMIT_ENTRIES)
            .unwrap_or_else(|_| panic!("bounded test partition count fits i32")))
            .map(|partition| response_partition(partition, 0))
            .collect(),
    );
    assert_eq!(
        normalize_group_offset_commit_response(
            one_partition_prepared(),
            &response(0, vec![oversized]),
        ),
        Err(GroupOffsetCommitProtocolFailure::ResultCount)
    );
    let too_many_empty_topics = (0..=MAX_GROUP_OFFSET_COMMIT_ENTRIES)
        .map(|index| response_topic(&format!("topic-{index}"), vec![]))
        .collect();
    assert_eq!(
        normalize_group_offset_commit_response(
            one_partition_prepared(),
            &response(0, too_many_empty_topics),
        ),
        Err(GroupOffsetCommitProtocolFailure::TopicCount)
    );
}

fn one_partition_prepared() -> super::PreparedGroupOffsetCommit {
    prepared(
        vec![entry(1, 0, 10, None)],
        4,
        vec![topic(1, Arc::from("orders"))],
    )
}

fn two_partition_prepared() -> super::PreparedGroupOffsetCommit {
    prepared(
        vec![entry(1, 0, 10, None), entry(2, 1, 20, None)],
        4,
        vec![topic(1, Arc::from("orders")), topic(2, Arc::from("audit"))],
    )
}

pub(super) fn response(
    throttle_time_ms: i32,
    topics: Vec<OffsetCommitResponseTopic>,
) -> OffsetCommitResponse {
    let mut response = OffsetCommitResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.topics = topics;
    response
}

pub(super) fn response_topic(
    name: &str,
    partitions: Vec<OffsetCommitResponsePartition>,
) -> OffsetCommitResponseTopic {
    let mut topic = OffsetCommitResponseTopic::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

pub(super) fn response_partition(
    partition_index: i32,
    error_code: i16,
) -> OffsetCommitResponsePartition {
    let mut partition = OffsetCommitResponsePartition::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    partition
}
