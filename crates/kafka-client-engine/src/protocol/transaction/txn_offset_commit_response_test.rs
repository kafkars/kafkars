//! Strict v4 transactional offset response-correlation scenarios.

use kafka_wire::{
    TxnOffsetCommitResponse,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use kafka_wire_core::Uuid;

use super::{
    TransactionBrokerCategory, TransactionOffsetCommitOutcome, TransactionOffsetCommitRef,
    TxnOffsetCommitResponseFailure, normalize_txn_offset_commit_v4_response,
};

#[test]
fn response_restores_caller_order_and_preserves_offsets_and_signed_errors() {
    let response = response(
        19,
        vec![
            topic("audit", vec![partition(1, -31_000)]),
            topic("orders", vec![partition(7, 47), partition(2, 0)]),
        ],
    );
    let expected = offsets();
    let normalized = normalize_txn_offset_commit_v4_response(&expected, &response)
        .unwrap_or_else(|error| panic!("response: {error:?}"));

    assert_eq!(normalized.throttle_time_ms(), 19);
    let results = normalized.offsets();
    assert_eq!(results.len(), expected.len());
    for (result, expected) in results.iter().zip(expected) {
        assert_eq!(result.offset(), expected);
    }
    assert_eq!(
        results[0].outcome(),
        TransactionOffsetCommitOutcome::Committed
    );
    assert_rejection(
        results[1].outcome(),
        -31_000,
        TransactionBrokerCategory::Rejected,
    );
    assert_rejection(results[2].outcome(), 47, TransactionBrokerCategory::Fenced);
}

#[test]
fn scalar_count_and_version_specific_shapes_are_rejected() {
    let expected = offsets();
    let mut negative_throttle = valid_response();
    negative_throttle.throttle_time_ms = -1;
    assert_eq!(
        normalize_txn_offset_commit_v4_response(&expected, &negative_throttle).err(),
        Some(TxnOffsetCommitResponseFailure::NegativeThrottleTime { actual: -1 })
    );

    let count = response(0, vec![topic("orders", vec![partition(2, 0)])]);
    assert_eq!(
        normalize_txn_offset_commit_v4_response(&expected, &count).err(),
        Some(TxnOffsetCommitResponseFailure::TopicCount {
            expected: 2,
            actual: 1
        })
    );

    let mut topic_id = valid_response();
    topic_id.topics[0].topic_id = Uuid::from_bytes([1; 16]);
    assert_eq!(
        normalize_txn_offset_commit_v4_response(&expected, &topic_id).err(),
        Some(TxnOffsetCommitResponseFailure::UnexpectedTopicId)
    );

    let empty_partitions = response(
        0,
        vec![
            topic("orders", Vec::new()),
            topic(
                "audit",
                vec![partition(1, 0), partition(2, 0), partition(7, 0)],
            ),
        ],
    );
    assert_eq!(
        normalize_txn_offset_commit_v4_response(&expected, &empty_partitions).err(),
        Some(TxnOffsetCommitResponseFailure::EmptyTopicPartitions)
    );
}

#[test]
fn duplicate_and_unmatched_identities_never_correlate() {
    let expected = offsets();
    let cases = [
        (
            response(
                0,
                vec![
                    topic("orders", vec![partition(2, 0), partition(7, 0)]),
                    topic("orders", vec![partition(1, 0)]),
                ],
            ),
            TxnOffsetCommitResponseFailure::DuplicateTopic,
        ),
        (
            response(
                0,
                vec![
                    topic("orders", vec![partition(2, 0), partition(2, 0)]),
                    topic("audit", vec![partition(1, 0)]),
                ],
            ),
            TxnOffsetCommitResponseFailure::DuplicatePartition { actual: 2 },
        ),
        (
            response(
                0,
                vec![
                    topic("orders", vec![partition(2, 0), partition(7, 0)]),
                    topic("payments", vec![partition(1, 0)]),
                ],
            ),
            TxnOffsetCommitResponseFailure::MissingTopic,
        ),
        (
            response(
                0,
                vec![
                    topic("orders", vec![partition(2, 0), partition(99, 0)]),
                    topic("audit", vec![partition(1, 0)]),
                ],
            ),
            TxnOffsetCommitResponseFailure::MissingPartition { actual: 7 },
        ),
        (
            response(
                0,
                vec![
                    topic("orders", vec![partition(-1, 0), partition(7, 0)]),
                    topic("audit", vec![partition(1, 0)]),
                ],
            ),
            TxnOffsetCommitResponseFailure::NegativePartition { actual: -1 },
        ),
    ];
    for (response, failure) in cases {
        assert_eq!(
            normalize_txn_offset_commit_v4_response(&expected, &response).err(),
            Some(failure)
        );
    }
}

#[test]
fn ambiguous_expected_offsets_are_rejected_before_response_binding() {
    let duplicate = [
        TransactionOffsetCommitRef::new("orders", 2, 93, None, None),
        TransactionOffsetCommitRef::new("orders", 2, 94, None, None),
    ];
    assert_eq!(
        normalize_txn_offset_commit_v4_response(&duplicate, &valid_response()).err(),
        Some(TxnOffsetCommitResponseFailure::DuplicateExpectedOffset { actual: 2 })
    );
}

fn assert_rejection(
    outcome: TransactionOffsetCommitOutcome,
    expected_code: i16,
    expected_category: TransactionBrokerCategory,
) {
    let TransactionOffsetCommitOutcome::Rejected(error) = outcome else {
        panic!("nonzero broker code must be rejected")
    };
    assert_eq!(error.code().get(), expected_code);
    assert_eq!(error.category(), expected_category);
}

fn offsets() -> [TransactionOffsetCommitRef<'static>; 3] {
    [
        TransactionOffsetCommitRef::new("orders", 2, 93, Some(7), Some("checkpoint-a")),
        TransactionOffsetCommitRef::new("audit", 1, 12, None, None),
        TransactionOffsetCommitRef::new("orders", 7, 120, Some(9), Some("")),
    ]
}

fn valid_response() -> TxnOffsetCommitResponse {
    response(
        0,
        vec![
            topic("orders", vec![partition(2, 0), partition(7, 0)]),
            topic("audit", vec![partition(1, 0)]),
        ],
    )
}

fn response(
    throttle_time_ms: i32,
    topics: Vec<TxnOffsetCommitResponseTopic>,
) -> TxnOffsetCommitResponse {
    let mut response = TxnOffsetCommitResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.topics = topics;
    response
}

fn topic(
    name: &str,
    partitions: Vec<TxnOffsetCommitResponsePartition>,
) -> TxnOffsetCommitResponseTopic {
    let mut topic = TxnOffsetCommitResponseTopic::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

fn partition(partition_index: i32, error_code: i16) -> TxnOffsetCommitResponsePartition {
    let mut partition = TxnOffsetCommitResponsePartition::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    partition
}
