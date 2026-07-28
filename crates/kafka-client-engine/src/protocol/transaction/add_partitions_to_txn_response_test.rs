//! Exact v3 transaction-partition response scenarios.

use kafka_wire::{
    AddPartitionsToTxnResponse,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
};

use super::{
    AddPartitionsToTxnPartitionOutcome, AddPartitionsToTxnResponseFailure,
    TransactionBrokerCategory, TransactionPartitionRef,
    normalize_add_partitions_to_txn_v3_response,
};

#[test]
fn response_restores_target_order_and_preserves_success_signed_errors_and_fencing() {
    let response = response(
        19,
        vec![
            topic("audit", vec![partition(1, -31_000)]),
            topic("orders", vec![partition(7, 47), partition(2, 0)]),
        ],
    );
    let normalized = normalize_add_partitions_to_txn_v3_response(&targets(), &response)
        .unwrap_or_else(|error| panic!("response: {error:?}"));

    let partitions = normalized.partitions();
    assert_eq!(
        partitions
            .iter()
            .map(|result| (result.topic(), result.partition()))
            .collect::<Vec<_>>(),
        [("orders", 2), ("audit", 1), ("orders", 7)]
    );
    assert_eq!(
        partitions[0].outcome(),
        AddPartitionsToTxnPartitionOutcome::Added
    );
    let AddPartitionsToTxnPartitionOutcome::Rejected(signed) = partitions[1].outcome() else {
        panic!("audit rejection must remain exact");
    };
    assert_eq!(signed.code().get(), -31_000);
    assert_eq!(signed.category(), TransactionBrokerCategory::Rejected);
    let AddPartitionsToTxnPartitionOutcome::Rejected(fenced) = partitions[2].outcome() else {
        panic!("producer fence must remain exact");
    };
    assert_eq!(fenced.code().get(), 47);
    assert_eq!(fenced.category(), TransactionBrokerCategory::Fenced);
}

#[test]
fn malformed_topic_and_partition_shapes_never_correlate() {
    let cases = [
        (
            response(
                0,
                vec![
                    topic("orders", vec![partition(2, 0), partition(7, 0)]),
                    topic("orders", vec![partition(1, 0)]),
                ],
            ),
            AddPartitionsToTxnResponseFailure::DuplicateTopic,
        ),
        (
            response(
                0,
                vec![
                    topic("orders", vec![partition(2, 0), partition(2, 0)]),
                    topic("audit", vec![partition(1, 0)]),
                ],
            ),
            AddPartitionsToTxnResponseFailure::DuplicatePartition { actual: 2 },
        ),
        (
            response(
                0,
                vec![
                    topic("orders", vec![partition(2, 0), partition(7, 0)]),
                    topic("payments", vec![partition(1, 0)]),
                ],
            ),
            AddPartitionsToTxnResponseFailure::MissingTopic,
        ),
        (
            response(
                0,
                vec![
                    topic("orders", vec![partition(2, 0), partition(99, 0)]),
                    topic("audit", vec![partition(1, 0)]),
                ],
            ),
            AddPartitionsToTxnResponseFailure::MissingPartition { actual: 7 },
        ),
    ];
    for (response, expected) in cases {
        assert_eq!(
            normalize_add_partitions_to_txn_v3_response(&targets(), &response).err(),
            Some(expected)
        );
    }
}

#[test]
fn v3_only_fields_and_scalar_shapes_are_enforced() {
    let mut negative_throttle = valid_response();
    negative_throttle.throttle_time_ms = -1;
    assert_eq!(
        normalize_add_partitions_to_txn_v3_response(&targets(), &negative_throttle).err(),
        Some(AddPartitionsToTxnResponseFailure::NegativeThrottleTime { actual: -1 })
    );

    let mut top_level = valid_response();
    top_level.error_code = -731;
    assert_eq!(
        normalize_add_partitions_to_txn_v3_response(&targets(), &top_level).err(),
        Some(AddPartitionsToTxnResponseFailure::UnexpectedTopLevelError { actual: -731 })
    );

    let empty_partition_topic = response(
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
        normalize_add_partitions_to_txn_v3_response(&targets(), &empty_partition_topic).err(),
        Some(AddPartitionsToTxnResponseFailure::EmptyTopicPartitions)
    );
}

fn targets() -> [TransactionPartitionRef<'static>; 3] {
    [
        TransactionPartitionRef::new("orders", 2),
        TransactionPartitionRef::new("audit", 1),
        TransactionPartitionRef::new("orders", 7),
    ]
}

fn valid_response() -> AddPartitionsToTxnResponse {
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
    topics: Vec<AddPartitionsToTxnTopicResult>,
) -> AddPartitionsToTxnResponse {
    let mut response = AddPartitionsToTxnResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.results_by_topic_v3_and_below = topics;
    response
}

fn topic(
    name: &str,
    partitions: Vec<AddPartitionsToTxnPartitionResult>,
) -> AddPartitionsToTxnTopicResult {
    let mut topic = AddPartitionsToTxnTopicResult::default();
    topic.name = name.into();
    topic.results_by_partition = partitions;
    topic
}

fn partition(partition_index: i32, error_code: i16) -> AddPartitionsToTxnPartitionResult {
    let mut partition = AddPartitionsToTxnPartitionResult::default();
    partition.partition_index = partition_index;
    partition.partition_error_code = error_code;
    partition
}
