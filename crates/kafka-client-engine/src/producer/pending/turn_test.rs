//! Bounded tombstone inspection and explicit turn-progress scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, Moment, PartitionIndex};

use super::{PendingAdmissionRegistry, ProducerSendFailure, ProducerSendFailureKind};
use crate::producer::{ProducerRecord, boundary::ProducerSend};

#[test]
fn take_next_counts_each_abandoned_tombstone_against_its_budget() {
    let mut registry = PendingAdmissionRegistry::new(3, 128, 3);
    let first = register(&mut registry, "first");
    let second = register(&mut registry, "second");
    let third = register(&mut registry, "third");
    drop(first);
    drop(second);

    let first_turn = registry
        .take_next(1)
        .unwrap_or_else(|error| panic!("first bounded take failed: {error:?}"));
    assert_eq!(first_turn.inspected(), 1);
    assert!(first_turn.remaining());
    assert!(first_turn.into_attempt().is_none());
    assert_eq!(registry.stats().records, 2);

    let second_turn = registry
        .take_next(1)
        .unwrap_or_else(|error| panic!("second bounded take failed: {error:?}"));
    assert_eq!(second_turn.inspected(), 1);
    assert!(second_turn.remaining());
    assert!(second_turn.into_attempt().is_none());
    assert_eq!(registry.stats().records, 1);

    let live = registry
        .take_next(1)
        .unwrap_or_else(|error| panic!("live bounded take failed: {error:?}"));
    assert_eq!(live.inspected(), 1);
    assert!(!live.remaining());
    let attempt = live
        .into_attempt()
        .unwrap_or_else(|| panic!("third admission should remain live"));
    settle(attempt, third);
}

#[test]
fn expire_and_drain_count_tombstones_without_yielding_fake_failures() {
    let mut expiring = abandoned_registry();
    let expired = expiring
        .expire_due(Moment::from_tick(20), 1)
        .unwrap_or_else(|error| panic!("bounded expiry failed: {error:?}"));
    assert!(expired.is_empty());
    assert_eq!(expired.inspected(), 1);
    assert!(expired.remaining());
    assert_eq!(expiring.stats().records, 2);

    let mut draining = abandoned_registry();
    draining.begin_close();
    let drained = draining
        .drain_closed(1)
        .unwrap_or_else(|error| panic!("bounded drain failed: {error:?}"));
    assert!(drained.is_empty());
    assert_eq!(drained.inspected(), 1);
    assert!(drained.remaining());
    assert_eq!(draining.stats().records, 2);
}

fn abandoned_registry() -> PendingAdmissionRegistry {
    let mut registry = PendingAdmissionRegistry::new(3, 128, 3);
    drop(register(&mut registry, "first"));
    drop(register(&mut registry, "second"));
    drop(register(&mut registry, "third"));
    registry
}

fn register(registry: &mut PendingAdmissionRegistry, topic: &str) -> ProducerSend {
    registry
        .register(
            ProducerRecord::new(
                Arc::from(topic),
                PartitionIndex::from_raw(0),
                1,
                None,
                Some(Bytes::from_static(b"value")),
            ),
            Deadline::from_tick(10),
            Instant::now(),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"))
        .into_send()
}

fn settle(attempt: super::PendingPromotionAttempt, send: ProducerSend) {
    let local = attempt
        .settle_local(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|_failure| panic!("live attempt should settle"));
    let (_admission, job) = local.into_parts();
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}
