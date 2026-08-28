//! Capacity synchronization scenarios for producer host construction.

use kafka_client_core::{
    ByteCount, Deadline, Moment, ProducerBatchPolicy, ProducerInput, ProducerRetryPolicy,
    execution_stop_effect_capacity, producer_transition_effect_capacity,
};
use std::sync::Arc;

use super::{
    ProducerHost, ProducerHostLimitError, ProducerHostLimits, ProducerHostStartError,
    ProducerStoreError,
    admission_test::{admit, record},
};

#[test]
fn valid_limits_construct_one_synchronized_host() {
    let limits = valid_limits();
    let host = start(limits);
    let stats = host.stats();

    assert_eq!(stats.store.records, 0);
    assert_eq!(stats.store.bytes, 0);
    assert_eq!(stats.store.batches, 0);
    assert_eq!(stats.core_retained_bytes, ByteCount::new(0));
    assert_eq!(stats.core_completion_slots, 0);
    assert_eq!(stats.active_timers, 0);
    assert_eq!(stats.prepared_batches, 0);
    assert_eq!(stats.prepared_bytes, 0);
    assert_eq!(stats.submission_deadlines, 0);
    assert_eq!(stats.pending_effects, 0);
    assert!(stats.healthy);
}

#[test]
fn topic_identity_capacity_covers_active_and_waiting_admissions() {
    let Ok(batch_policy) = ProducerBatchPolicy::try_new(1, ByteCount::new(64), 100) else {
        panic!("test policy should be valid")
    };
    let mut limits = valid_limits();
    limits.completion_capacity = 1;
    limits.record_capacity = 1;
    limits.batch_capacity = 1;
    limits.timer_capacity = 1;
    limits.waiting_record_capacity = 2;
    limits.batch_policy = batch_policy;
    let mut host = start(limits);

    for topic in ["orders", "payments", "refunds"] {
        let id = host
            .store
            .retain_waiting_topic(Arc::from(topic))
            .unwrap_or_else(|error| panic!("{topic} identity should fit: {error}"));
        host.store
            .release_waiting_topic(id)
            .unwrap_or_else(|error| panic!("{topic} identity should release: {error}"));
    }

    assert_eq!(
        host.store.retain_waiting_topic(Arc::from("shipments")),
        Err(ProducerStoreError::TopicIdentityExhausted)
    );
}

#[test]
fn zero_and_mismatched_admission_capacities_are_rejected() {
    let mut zero_bytes = valid_limits();
    zero_bytes.retained_bytes = 0;
    assert_limit(zero_bytes, ProducerHostLimitError::ZeroRetainedBytes);

    let mut zero_completions = valid_limits();
    zero_completions.completion_capacity = 0;
    assert_limit(
        zero_completions,
        ProducerHostLimitError::ZeroCompletionCapacity,
    );

    let mut zero_waiting_records = valid_limits();
    zero_waiting_records.waiting_record_capacity = 0;
    assert_limit(
        zero_waiting_records,
        ProducerHostLimitError::ZeroWaitingRecordCapacity,
    );

    let mut zero_waiting_bytes = valid_limits();
    zero_waiting_bytes.waiting_byte_capacity = 0;
    assert_limit(
        zero_waiting_bytes,
        ProducerHostLimitError::ZeroWaitingByteCapacity,
    );

    let mut mismatched = valid_limits();
    mismatched.record_capacity = 1;
    assert_limit(mismatched, ProducerHostLimitError::RecordCompletionMismatch);
}

#[test]
fn downstream_mechanisms_must_cover_admission_capacity() {
    let mut batches = valid_limits();
    batches.batch_capacity = 1;
    assert_limit(batches, ProducerHostLimitError::InsufficientBatchCapacity);

    let mut timers = valid_limits();
    timers.timer_capacity = 1;
    assert_limit(timers, ProducerHostLimitError::InsufficientTimerCapacity);
}

#[test]
fn batching_count_policy_cannot_exceed_record_capacity() {
    let Ok(policy) = ProducerBatchPolicy::try_new(3, ByteCount::new(64), 10) else {
        panic!("test policy should be valid")
    };
    let mut limits = valid_limits();
    limits.batch_policy = policy;

    assert_limit(
        limits,
        ProducerHostLimitError::BatchRecordLimitExceedsCapacity,
    );
}

#[test]
fn encoded_and_wire_byte_limits_must_be_nonzero() {
    let mut encoded = valid_limits();
    encoded.encoded_byte_capacity = 0;
    assert_limit(encoded, ProducerHostLimitError::ZeroEncodedByteCapacity);

    let mut wire = valid_limits();
    wire.max_wire_batch_bytes = 0;
    assert_limit(wire, ProducerHostLimitError::ZeroWireBatchBytes);

    let mut request = valid_limits();
    request.max_request_bytes = 0;
    assert_limit(request, ProducerHostLimitError::ZeroRequestBytes);
}

#[test]
fn request_bytes_must_cover_one_wire_batch() {
    let mut limits = valid_limits();
    limits.max_request_bytes = limits.max_wire_batch_bytes - 1;

    assert_limit(limits, ProducerHostLimitError::RequestSmallerThanBatch);
}

#[test]
fn idempotent_in_flight_requests_are_configurable_only_through_five() {
    let mut zero = valid_limits();
    zero.max_in_flight_requests_per_broker = 0;
    assert_limit(zero, ProducerHostLimitError::ZeroInFlightRequests);

    let mut six = valid_limits();
    six.max_in_flight_requests_per_broker = 6;
    assert_limit(six, ProducerHostLimitError::TooManyInFlightRequests);
}

#[test]
fn validated_limits_cover_the_maximal_core_stop_shape() {
    let limits = valid_limits();
    let mut host = start(limits);
    let first = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    let second = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("payments"),
    );
    for _flush in 0..limits.completion_capacity {
        let accepted = host
            .core
            .apply(ProducerInput::FlushRequested)
            .unwrap_or_else(|error| panic!("flush reservation should succeed: {error}"));
        assert_eq!(accepted.effects().len(), 1);
    }

    let transition = host
        .core
        .apply(ProducerInput::ExecutionUnavailable)
        .unwrap_or_else(|error| panic!("maximal terminal plan should succeed: {error}"));
    let total_completion_capacity = limits
        .completion_capacity
        .checked_add(limits.waiting_record_capacity)
        .unwrap_or_else(|| panic!("validated total completion capacity must be representable"));
    let transition_capacity =
        producer_transition_effect_capacity(total_completion_capacity, limits.completion_capacity)
            .unwrap_or_else(|| panic!("validated host capacity must be representable"));
    assert_eq!(
        transition.effects().len(),
        execution_stop_effect_capacity(limits.record_capacity, limits.completion_capacity)
            .unwrap_or_else(|| panic!("validated host capacity must be representable"))
    );
    assert_eq!(
        host.core.transition_effect_capacity(),
        Some(transition_capacity)
    );
    assert!(transition.effects().len() <= transition_capacity);
    drop((first, second));
}

#[test]
fn combined_transition_capacity_overflow_is_rejected_before_allocation() {
    let mut limits = valid_limits();
    limits.completion_capacity = usize::MAX - 1;
    limits.waiting_record_capacity = 1;
    limits.record_capacity = usize::MAX - 1;
    limits.batch_capacity = usize::MAX - 1;
    limits.timer_capacity = usize::MAX - 1;

    assert_eq!(
        execution_stop_effect_capacity(limits.record_capacity, limits.completion_capacity),
        None
    );
    assert_eq!(
        producer_transition_effect_capacity(limits.record_capacity, limits.completion_capacity),
        None
    );
    assert_limit(limits, ProducerHostLimitError::TransitionCapacityOverflow);
}

#[test]
fn combined_record_capacity_overflow_is_rejected_before_allocation() {
    let mut limits = valid_limits();
    limits.completion_capacity = usize::MAX;
    limits.waiting_record_capacity = 1;
    limits.record_capacity = usize::MAX;
    limits.batch_capacity = usize::MAX;
    limits.timer_capacity = usize::MAX;

    assert_limit(limits, ProducerHostLimitError::TotalRecordCapacityOverflow);
}

#[test]
fn combined_retained_byte_capacity_overflow_is_rejected_before_allocation() {
    let mut limits = valid_limits();
    limits.retained_bytes = usize::MAX;
    limits.waiting_byte_capacity = 1;

    assert_limit(limits, ProducerHostLimitError::TotalRetainedBytesOverflow);
}

pub(crate) fn valid_limits() -> ProducerHostLimits {
    let Ok(batch_policy) = ProducerBatchPolicy::try_new(2, ByteCount::new(64), 100) else {
        panic!("test policy should be valid")
    };
    ProducerHostLimits {
        retained_bytes: 128,
        completion_capacity: 2,
        waiting_record_capacity: 2,
        waiting_byte_capacity: 128,
        record_capacity: 2,
        batch_capacity: 2,
        timer_capacity: 2,
        encoded_byte_capacity: 1_024,
        max_wire_batch_bytes: 1_024,
        max_request_bytes: 1_024,
        max_in_flight_requests_per_broker: 5,
        batch_policy,
        retry_policy: ProducerRetryPolicy::none(),
        compression: kafka_client_core::CompressionPolicy::None,
        compression_worker_count: 0,
        compression_job_capacity: 0,
        compression_byte_capacity: 0,
    }
}

#[test]
fn impossible_admission_allocation_maps_to_start_error() {
    let capacity = usize::MAX / 6;
    let mut limits = valid_limits();
    limits.completion_capacity = capacity;
    limits.record_capacity = capacity;
    limits.batch_capacity = capacity;
    limits.timer_capacity = capacity;

    match ProducerHost::new(limits) {
        Err(ProducerHostStartError::Allocation) => {}
        Err(error) => panic!("unexpected producer host start failure: {error}"),
        Ok(_host) => panic!("impossible producer admission allocation should fail"),
    }
}

pub(crate) fn start(limits: ProducerHostLimits) -> ProducerHost {
    match ProducerHost::new(limits) {
        Ok(host) => host,
        Err(error) => panic!("producer host should start: {error}"),
    }
}

fn assert_limit(limits: ProducerHostLimits, expected: ProducerHostLimitError) {
    match ProducerHost::new(limits) {
        Err(ProducerHostStartError::Limits(actual)) => assert_eq!(actual, expected),
        Err(error) => panic!("unexpected producer host start failure: {error}"),
        Ok(_host) => panic!("invalid producer host limits should fail"),
    }
}
