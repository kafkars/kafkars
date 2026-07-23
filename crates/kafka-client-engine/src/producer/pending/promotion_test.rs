//! Drop-versus-promotion linearization and exact record ownership scenarios.

use std::{sync::Arc, task::Poll, thread};

use bytes::Bytes;
use kafka_client_core::{
    Deadline, DeliveryStatus, PartitionIndex, ProducerCompletion, ProducerFailure,
};

use super::{
    PendingAdmissionRegistry, PendingCellError,
    test_support::{CountingWake, poll_send},
};
use crate::{ProducerDeliveryObserver, completion::CompletionRegistry, producer::ProducerRecord};

#[test]
fn drop_before_promotion_tombstones_and_preserves_the_unadmitted_record() {
    let mut pending = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = pending
        .register(
            record("orders"),
            Deadline::from_tick(10),
            std::time::Instant::now(),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"));
    let id = registration.id();
    drop(registration.into_send());
    let entry = pending
        .cancel(id)
        .unwrap_or_else(|error| panic!("tombstoned entry should remain removable: {error:?}"));

    assert!(matches!(
        entry.begin_promotion(),
        Err(PendingCellError::Abandoned)
    ));
    assert_eq!(entry.into_record().topic().as_ref(), "orders");
    assert_eq!(pending.stats().records, 0);
    assert_eq!(pending.stats().retained_bytes, 0);
}

#[test]
fn promotion_before_drop_abandons_only_the_accepted_observer() {
    let cell = super::PendingSendCell::new_for_test();
    let mut send = crate::producer::boundary::ProducerSend::from_pending(cell.clone());
    let wake = CountingWake::new();
    assert_eq!(poll_send(&mut send, wake), Poll::Pending);
    let claim = cell
        .begin_promotion()
        .unwrap_or_else(|error| panic!("promotion should claim: {error:?}"));
    let mut completions = super::test_support::notification_registry(2);
    let (id, observer) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("accepted completion should reserve: {error}"));
    let job = claim
        .accept(ProducerDeliveryObserver::from_completion(observer))
        .unwrap_or_else(|(_promotion, _observer)| {
            panic!("promotion should install accepted observer")
        });

    drop(send);
    notify(&completions, job);
    assert_eq!(
        completions.publish(
            id,
            ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
                DeliveryStatus::NotSent,
            )),
        ),
        Ok(())
    );
    wait_for_reclaim(&mut completions, id);
    stop(&mut completions);
}

#[test]
fn drop_after_promotion_claim_is_dispatched_off_the_caller_thread() {
    let caller = thread::current().id();
    let cell = super::PendingSendCell::new_for_test();
    let send = crate::producer::boundary::ProducerSend::from_pending(cell.clone());
    let claim = cell
        .begin_promotion()
        .unwrap_or_else(|error| panic!("promotion should claim: {error:?}"));
    drop(send);
    let mut completions = super::test_support::notification_registry(2);
    let (id, observer) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("accepted completion should reserve: {error}"));
    let job = claim
        .accept(ProducerDeliveryObserver::from_completion(observer))
        .unwrap_or_else(|(_promotion, _observer)| {
            panic!("claimed promotion should remain accepted")
        });
    notify(&completions, job);
    assert_eq!(
        completions.publish(
            id,
            ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
                DeliveryStatus::NotSent,
            )),
        ),
        Ok(())
    );
    wait_for_reclaim(&mut completions, id);
    assert_ne!(
        completions
            .notifier_thread_id()
            .unwrap_or_else(|| panic!("notifier thread should still be recorded")),
        caller
    );
    stop(&mut completions);
}

fn record(topic: &str) -> ProducerRecord {
    ProducerRecord::new(
        Arc::from(topic),
        PartitionIndex::from_raw(0),
        1,
        None,
        Some(Bytes::from_static(b"value")),
    )
}

fn wait_for_reclaim(
    registry: &mut CompletionRegistry<ProducerCompletion>,
    expected: crate::completion::CompletionId,
) {
    for _attempt in 0..10_000 {
        match registry.next_reclaim() {
            Ok(Some(actual)) => {
                assert_eq!(actual, expected);
                assert_eq!(
                    registry.finish_reclaim(actual),
                    Ok(crate::completion::ReclaimStatus::Reclaimed)
                );
                return;
            }
            Ok(None) => thread::yield_now(),
            Err(error) => panic!("reclaim should remain connected: {error}"),
        }
    }
    panic!("accepted completion should become reclaimable");
}

fn stop(registry: &mut CompletionRegistry<ProducerCompletion>) {
    let join = registry
        .stop_notifier()
        .unwrap_or_else(|error| panic!("completion notifier should stop: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}

fn notify(registry: &CompletionRegistry<ProducerCompletion>, job: super::PendingNotificationJob) {
    if let Err((error, _job)) = registry.notify_pending(job) {
        panic!("pending notification should queue: {error}");
    }
}
