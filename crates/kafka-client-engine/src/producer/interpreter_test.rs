//! Effect ordering, timer expiry, release, and terminal publication scenarios.

use kafka_client_core::{
    ByteCount, Deadline, DeliveryStatus, Moment, ProducerBatchPolicy, ProducerCompletion,
    ProducerEffect, ProducerFailureKind,
};

use super::{
    ProducerHostLimits,
    admission_test::{admit, record},
    host_limits_test::{start, valid_limits},
};

#[test]
fn generated_accumulation_waits_until_the_whole_admission_transition_drains() {
    let Ok(batch_policy) = ProducerBatchPolicy::try_new(1, ByteCount::new(u64::MAX), 10) else {
        panic!("test policy should be valid")
    };
    let limits = ProducerHostLimits {
        retained_bytes: 64,
        completion_capacity: 1,
        record_capacity: 1,
        batch_capacity: 1,
        timer_capacity: 1,
        notification_capacity: 1,
        batch_policy,
    };
    let mut host = start(limits);
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(50),
        record("orders"),
    );

    assert_eq!(host.stats().active_timers, 0);
    assert_eq!(host.stats().pending_effects, 1);
    assert!(matches!(
        host.pending_effects(),
        [ProducerEffect::MaterializeBatch { .. }]
    ));
    assert_eq!(host.retry_pending_completions(), Ok(0));
    assert_eq!(host.stats().pending_effects, 1);
    drop(admitted);
}

#[test]
fn due_deadline_releases_engine_bytes_before_publishing_terminal_failure() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let operation_id = admitted.operation_id();
    let observer = admitted.into_observer();

    assert_eq!(host.fire_due(Moment::from_tick(5)), Ok(1));
    let stats = host.stats();
    assert_eq!(stats.store.records, 0);
    assert_eq!(stats.store.bytes, 0);
    assert_eq!(stats.store.batches, 0);
    assert_eq!(stats.core_retained_bytes, ByteCount::new(0));
    assert_eq!(stats.core_completion_slots, 1);
    assert_eq!(stats.active_timers, 0);
    assert_eq!(stats.pending_effects, 0);

    let completion = match observer.wait() {
        Ok(completion) => completion,
        Err(error) => panic!("terminal completion should be observable: {error}"),
    };
    let ProducerCompletion::Failed(failure) = completion else {
        panic!("deadline should fail the producer operation")
    };
    assert_eq!(failure.kind(), ProducerFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    assert_eq!(operation_id.get(), 1);
}
