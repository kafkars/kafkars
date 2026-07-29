//! Successful active-producer normalization and ordering scenarios.

use kafka_client_core::AdminDescribeProducerTarget;
use kafka_wire::{
    DescribeProducersResponse,
    describe_producers_response::{PartitionResponse, ProducerState, TopicResponse},
};

use super::{
    NormalizedDescribeProducerResult, NormalizedDescribeProducersResponse,
    normalize_describe_producers_response,
};

const RETAINED_LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn success_preserves_scalars_sentinels_and_strict_producer_order() {
    let generated = response(
        23,
        0,
        None,
        vec![
            producer(9, 3, 17, 1_234, 4, 82),
            producer(2, 1, -1, -1, 0, -1),
        ],
    );
    let normalized = normalize(&generated, RETAINED_LIMIT)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    assert_eq!(normalized.throttle_time_ms(), 23);
    assert!(normalized.retained_bytes() > 0);
    let NormalizedDescribeProducerResult::Described(states) = normalized.result() else {
        panic!("success became broker error");
    };
    assert_eq!(states.len(), 2);
    assert_eq!(states[0].into_parts(), (2, 1, -1, -1, 0, None));
    assert_eq!(states[1].into_parts(), (9, 3, 17, 1_234, 4, Some(82)));
}

#[test]
fn empty_active_producer_set_is_a_valid_bounded_success() {
    let normalized = normalize(&response(0, 0, None, Vec::new()), RETAINED_LIMIT)
        .unwrap_or_else(|error| panic!("empty producer set: {error:?}"));
    let NormalizedDescribeProducerResult::Described(states) = normalized.result() else {
        panic!("empty success became error");
    };
    assert!(states.is_empty());
    let (throttle, result, retained) = normalized.into_parts();
    assert_eq!(throttle, 0);
    assert!(matches!(
        result,
        NormalizedDescribeProducerResult::Described(states) if states.is_empty()
    ));
    assert!(retained > 0);
}

pub(super) fn target() -> AdminDescribeProducerTarget {
    AdminDescribeProducerTarget::new("audit-log".to_owned(), 7)
}

pub(super) fn normalize(
    response: &DescribeProducersResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeProducersResponse, super::DescribeProducersProtocolFailure> {
    normalize_describe_producers_response(&target(), 0, response, retained_limit)
}

pub(super) fn producer(
    producer_id: i64,
    producer_epoch: i32,
    last_sequence: i32,
    last_timestamp: i64,
    coordinator_epoch: i32,
    current_txn_start_offset: i64,
) -> ProducerState {
    let mut state = ProducerState::default();
    state.producer_id = producer_id;
    state.producer_epoch = producer_epoch;
    state.last_sequence = last_sequence;
    state.last_timestamp = last_timestamp;
    state.coordinator_epoch = coordinator_epoch;
    state.current_txn_start_offset = current_txn_start_offset;
    state
}

pub(super) fn response(
    throttle_time_ms: i32,
    error_code: i16,
    error_message: Option<String>,
    active_producers: Vec<ProducerState>,
) -> DescribeProducersResponse {
    let mut partition = PartitionResponse::default();
    partition.partition_index = 7;
    partition.error_code = error_code;
    partition.error_message = error_message.map(Into::into);
    partition.active_producers = active_producers;
    let mut topic = TopicResponse::default();
    topic.name = "audit-log".into();
    topic.partitions = vec![partition];
    let mut response = DescribeProducersResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.topics = vec![topic];
    response
}
