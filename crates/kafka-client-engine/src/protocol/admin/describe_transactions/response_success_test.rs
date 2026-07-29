//! Successful API-key 65 normalization and canonical ordering scenarios.

use kafka_wire::{
    DescribeTransactionsResponse,
    describe_transactions_response::{TopicData, TransactionState},
};

use super::{
    NormalizedDescribeTransactionResult, NormalizedDescribeTransactionsResponse,
    normalize_describe_transactions_response,
};

pub(super) const RETAINED_LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn success_preserves_signed_scalars_sentinel_and_canonical_nested_order() {
    let generated = response(
        23,
        state(
            0,
            "invoice-worker",
            "Ongoing",
            -1,
            -1,
            -1,
            -1,
            vec![topic("orders", vec![2, 0]), topic("audit", vec![3, 1])],
        ),
    );
    let normalized = normalize(&generated, RETAINED_LIMIT)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    assert_eq!(normalized.throttle_time_ms(), 23);
    assert!(normalized.retained_bytes() > 0);
    let NormalizedDescribeTransactionResult::Described(description) = normalized.result() else {
        panic!("success became broker error");
    };
    assert_eq!(description.transaction_state(), "Ongoing");
    assert_eq!(description.scalar_parts(), (-1, None, -1, -1));
    assert_eq!(description.topics()[0].topic(), "audit");
    assert_eq!(description.topics()[0].partitions(), [1, 3]);
    assert_eq!(description.topics()[1].topic(), "orders");
    assert_eq!(description.topics()[1].partitions(), [0, 2]);
}

#[test]
fn present_start_and_empty_topic_set_remain_exact_bounded_facts() {
    let normalized = normalize(
        &response(
            0,
            state(0, "invoice-worker", "Empty", 60_000, 99, 91, 7, Vec::new()),
        ),
        RETAINED_LIMIT,
    )
    .unwrap_or_else(|error| panic!("empty transaction: {error:?}"));
    let (throttle, result, retained) = normalized.into_parts();
    assert_eq!(throttle, 0);
    let NormalizedDescribeTransactionResult::Described(description) = result else {
        panic!("success became broker error");
    };
    assert_eq!(description.scalar_parts(), (60_000, Some(99), 91, 7));
    assert!(description.topics().is_empty());
    assert!(retained > 0);
}

pub(super) fn normalize(
    response: &DescribeTransactionsResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeTransactionsResponse, super::DescribeTransactionsProtocolFailure> {
    normalize_describe_transactions_response("invoice-worker", 0, response, retained_limit)
}

pub(super) fn response(
    throttle_time_ms: i32,
    transaction_state: TransactionState,
) -> DescribeTransactionsResponse {
    let mut response = DescribeTransactionsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.transaction_states = vec![transaction_state];
    response
}

#[allow(clippy::too_many_arguments)]
pub(super) fn state(
    error_code: i16,
    transactional_id: &str,
    transaction_state: &str,
    transaction_timeout_ms: i32,
    transaction_start_time_ms: i64,
    producer_id: i64,
    producer_epoch: i16,
    topics: Vec<TopicData>,
) -> TransactionState {
    let mut state = TransactionState::default();
    state.error_code = error_code;
    state.transactional_id = transactional_id.into();
    state.transaction_state = transaction_state.into();
    state.transaction_timeout_ms = transaction_timeout_ms;
    state.transaction_start_time_ms = transaction_start_time_ms;
    state.producer_id = producer_id;
    state.producer_epoch = producer_epoch;
    state.topics = topics;
    state
}

pub(super) fn topic(topic: &str, partitions: Vec<i32>) -> TopicData {
    let mut data = TopicData::default();
    data.topic = topic.into();
    data.partitions = partitions;
    data
}
