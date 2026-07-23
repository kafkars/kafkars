//! Capacity synchronization scenarios for producer host construction.

use kafka_client_core::{
    ByteCount, Deadline, Moment, ProducerBatchPolicy, ProducerInput,
    execution_stop_effect_capacity, producer_transition_effect_capacity,
};

use super::{
    ProducerHost, ProducerHostLimitError, ProducerHostLimits, ProducerHostStartError,
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
}

#[test]
fn configured_terminal_tail_covers_the_maximal_core_stop_shape() {
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
    let transition_capacity =
        producer_transition_effect_capacity(limits.record_capacity, limits.completion_capacity)
            .unwrap_or_else(|| panic!("validated host capacity must be representable"));
    assert_eq!(
        transition.effects().len(),
        execution_stop_effect_capacity(limits.record_capacity, limits.completion_capacity)
            .unwrap_or_else(|| panic!("validated host capacity must be representable"))
    );
    assert_eq!(host.fatal_transition.capacity(), transition_capacity);
    assert_eq!(
        host.terminal_quarantine.transition_effect_capacity(),
        transition_capacity
    );
    assert!(transition.effects().len() <= transition_capacity);
    drop((first, second));
}

#[test]
fn combined_transition_capacity_overflow_is_rejected_before_allocation() {
    let mut limits = valid_limits();
    limits.completion_capacity = usize::MAX;
    limits.record_capacity = usize::MAX;
    limits.batch_capacity = usize::MAX;
    limits.timer_capacity = usize::MAX;

    assert_eq!(
        execution_stop_effect_capacity(limits.record_capacity, limits.completion_capacity),
        None
    );
    assert_eq!(
        producer_transition_effect_capacity(limits.record_capacity, limits.completion_capacity),
        None
    );
    assert_limit(limits, ProducerHostLimitError::TerminalTailCapacityOverflow);
}

pub(super) fn valid_limits() -> ProducerHostLimits {
    let Ok(batch_policy) = ProducerBatchPolicy::try_new(2, ByteCount::new(64), 100) else {
        panic!("test policy should be valid")
    };
    ProducerHostLimits {
        retained_bytes: 128,
        completion_capacity: 2,
        record_capacity: 2,
        batch_capacity: 2,
        timer_capacity: 2,
        encoded_byte_capacity: 1_024,
        max_wire_batch_bytes: 1_024,
        batch_policy,
    }
}

pub(super) fn start(limits: ProducerHostLimits) -> ProducerHost {
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
