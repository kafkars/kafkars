//! Capacity synchronization scenarios for producer host construction.

use kafka_client_core::{ByteCount, ProducerBatchPolicy};

use super::{ProducerHost, ProducerHostLimitError, ProducerHostLimits, ProducerHostStartError};

#[test]
fn valid_limits_construct_one_synchronized_host() {
    let host = start(valid_limits());
    let stats = host.stats();

    assert_eq!(stats.store.records, 0);
    assert_eq!(stats.store.bytes, 0);
    assert_eq!(stats.store.batches, 0);
    assert_eq!(stats.core_retained_bytes, ByteCount::new(0));
    assert_eq!(stats.core_completion_slots, 0);
    assert_eq!(stats.active_timers, 0);
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

    let mut notifications = valid_limits();
    notifications.notification_capacity = 1;
    assert_limit(
        notifications,
        ProducerHostLimitError::InsufficientNotificationCapacity,
    );
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
        notification_capacity: 2,
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
