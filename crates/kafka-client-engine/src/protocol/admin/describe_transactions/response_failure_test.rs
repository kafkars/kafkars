//! Error, hostile shape, scalar, count, and capacity scenarios for API 65.

use kafka_client_core::{
    DESCRIBE_TRANSACTIONS_MAX_PARTITIONS, DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES,
    DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES, DESCRIBE_TRANSACTIONS_MAX_TOPICS,
};
use kafka_wire::{DescribeTransactionsResponse, describe_transactions_response::TransactionState};

use super::{
    DescribeTransactionsProtocolFailure, NormalizedDescribeTransactionResult,
    normalize_describe_transactions_response,
    response_success_test::{RETAINED_LIMIT, normalize, response, state, topic},
};

#[test]
fn exact_version_throttle_and_transactional_id_correlation_are_strict() {
    let valid = success_state();
    for actual in [-1, 1, i16::MAX] {
        assert_eq!(
            normalize_describe_transactions_response(
                "invoice-worker",
                actual,
                &response(0, valid.clone()),
                RETAINED_LIMIT,
            ),
            Err(DescribeTransactionsProtocolFailure::UnsupportedApiVersion { actual })
        );
    }
    assert_eq!(
        normalize(&response(-1, valid.clone()), RETAINED_LIMIT),
        Err(DescribeTransactionsProtocolFailure::NegativeThrottleTime { actual: -1 })
    );

    let mut none = DescribeTransactionsResponse::default();
    none.throttle_time_ms = 0;
    assert_eq!(
        normalize(&none, RETAINED_LIMIT),
        Err(DescribeTransactionsProtocolFailure::UnexpectedTransactionStateCount { actual: 0 })
    );
    let mut multiple = response(0, valid.clone());
    multiple.transaction_states.push(valid.clone());
    assert_eq!(
        normalize(&multiple, RETAINED_LIMIT),
        Err(DescribeTransactionsProtocolFailure::UnexpectedTransactionStateCount { actual: 2 })
    );
    let mut wrong_id = valid;
    wrong_id.transactional_id = "other".into();
    assert_eq!(
        normalize(&response(0, wrong_id), RETAINED_LIMIT),
        Err(DescribeTransactionsProtocolFailure::UnexpectedTransactionalId)
    );
}

#[test]
fn broker_error_preserves_exact_code_and_rejects_ambiguous_success_payloads() {
    let normalized = normalize(&response(7, broker_error(-32_000)), RETAINED_LIMIT)
        .unwrap_or_else(|error| panic!("broker error: {error:?}"));
    assert_eq!(normalized.throttle_time_ms(), 7);
    let NormalizedDescribeTransactionResult::BrokerFailed(error) = normalized.result() else {
        panic!("broker error became success");
    };
    assert_eq!(error.code(), -32_000);

    let cases = [
        (
            state(1, "invoice-worker", "Empty", 0, 0, 0, 0, Vec::new()),
            "transaction_state",
        ),
        (
            state(1, "invoice-worker", "", 1, 0, 0, 0, Vec::new()),
            "transaction_timeout_ms",
        ),
        (
            state(1, "invoice-worker", "", 0, -1, 0, 0, Vec::new()),
            "transaction_start_time_ms",
        ),
        (
            state(1, "invoice-worker", "", 0, 0, -1, 0, Vec::new()),
            "producer_id",
        ),
        (
            state(1, "invoice-worker", "", 0, 0, 0, -1, Vec::new()),
            "producer_epoch",
        ),
        (
            state(
                1,
                "invoice-worker",
                "",
                0,
                0,
                0,
                0,
                vec![topic("orders", vec![0])],
            ),
            "topics",
        ),
    ];
    for (generated, field) in cases {
        assert_eq!(
            normalize(&response(0, generated), RETAINED_LIMIT),
            Err(DescribeTransactionsProtocolFailure::SuccessPayloadWithBrokerError { field })
        );
    }
}

#[test]
fn transaction_state_start_and_topic_partition_shapes_are_validated() {
    for (generated, expected) in [
        (
            state(0, "invoice-worker", "", 0, -1, 0, 0, Vec::new()),
            DescribeTransactionsProtocolFailure::EmptyTransactionState,
        ),
        (
            state(
                0,
                "invoice-worker",
                &"x".repeat(DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES + 1),
                0,
                -1,
                0,
                0,
                Vec::new(),
            ),
            DescribeTransactionsProtocolFailure::TransactionStateTooLong {
                actual: DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES + 1,
                max: DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES,
            },
        ),
        (
            state(0, "invoice-worker", "Empty", 0, -2, 0, 0, Vec::new()),
            DescribeTransactionsProtocolFailure::InvalidTransactionStartTime { actual: -2 },
        ),
        (
            success_with_topics(vec![topic("", vec![0])]),
            DescribeTransactionsProtocolFailure::EmptyTopic,
        ),
        (
            success_with_topics(vec![topic(&"t".repeat(250), vec![0])]),
            DescribeTransactionsProtocolFailure::TopicTooLong {
                actual: 250,
                max: 249,
            },
        ),
        (
            success_with_topics(vec![topic("orders", Vec::new())]),
            DescribeTransactionsProtocolFailure::EmptyPartitions,
        ),
        (
            success_with_topics(vec![topic("orders", vec![-1])]),
            DescribeTransactionsProtocolFailure::NegativePartition { actual: -1 },
        ),
    ] {
        assert_eq!(
            normalize(&response(0, generated), RETAINED_LIMIT),
            Err(expected)
        );
    }
}

#[test]
fn duplicates_hostile_counts_bytes_and_retained_capacity_are_rejected() {
    assert_eq!(
        normalize(
            &response(0, success_with_topics(vec![topic("orders", vec![1, 1])]),),
            RETAINED_LIMIT,
        ),
        Err(DescribeTransactionsProtocolFailure::DuplicatePartition { actual: 1 })
    );
    assert_eq!(
        normalize(
            &response(
                0,
                success_with_topics(vec![topic("orders", vec![0]), topic("orders", vec![1])]),
            ),
            RETAINED_LIMIT,
        ),
        Err(DescribeTransactionsProtocolFailure::DuplicateTopic)
    );

    let too_many_topics = (0..=DESCRIBE_TRANSACTIONS_MAX_TOPICS)
        .map(|index| topic(&format!("topic-{index}"), vec![0]))
        .collect();
    assert_eq!(
        normalize(
            &response(0, success_with_topics(too_many_topics)),
            RETAINED_LIMIT,
        ),
        Err(DescribeTransactionsProtocolFailure::TooManyTopics {
            actual: DESCRIBE_TRANSACTIONS_MAX_TOPICS + 1,
            max: DESCRIBE_TRANSACTIONS_MAX_TOPICS,
        })
    );
    assert_eq!(
        normalize(
            &response(
                0,
                success_with_topics(vec![topic(
                    "orders",
                    vec![0; DESCRIBE_TRANSACTIONS_MAX_PARTITIONS + 1],
                )]),
            ),
            RETAINED_LIMIT,
        ),
        Err(DescribeTransactionsProtocolFailure::TooManyPartitions {
            actual: DESCRIBE_TRANSACTIONS_MAX_PARTITIONS + 1,
            max: DESCRIBE_TRANSACTIONS_MAX_PARTITIONS,
        })
    );

    let topic_count = DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES / 249 + 1;
    let topics = (0..topic_count)
        .map(|index| {
            let prefix = format!("{index:07}-");
            topic(
                &format!("{prefix}{}", "x".repeat(249 - prefix.len())),
                vec![0],
            )
        })
        .collect();
    assert!(matches!(
        normalize(&response(0, success_with_topics(topics)), RETAINED_LIMIT,),
        Err(DescribeTransactionsProtocolFailure::TopicBytesExceeded { .. })
    ));
    assert!(matches!(
        normalize(&response(0, success_state()), 0),
        Err(DescribeTransactionsProtocolFailure::RetainedBytes {
            required,
            limit: 0,
        }) if required > 0
    ));
}

fn success_state() -> TransactionState {
    success_with_topics(Vec::new())
}

fn success_with_topics(
    topics: Vec<kafka_wire::describe_transactions_response::TopicData>,
) -> TransactionState {
    state(0, "invoice-worker", "Empty", -1, -1, -1, -1, topics)
}

fn broker_error(code: i16) -> TransactionState {
    state(code, "invoice-worker", "", 0, 0, 0, 0, Vec::new())
}
