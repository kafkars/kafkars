//! Pending cell waker, wait, and local-failure scenarios.

use std::{task::Poll, thread};

use kafka_client_core::{DeliveryStatus, ProducerCompletion, ProducerFailure};

use super::{
    PendingNotificationJob, PendingSendCell, ProducerSendFailure, ProducerSendFailureKind,
    test_support::{CountingWake, GateWake, poll_completion, poll_send},
};
use crate::{
    ProducerDeliveryStatus, ProducerSendError, completion::CompletionRegistry,
    producer::boundary::ProducerSend,
};

#[test]
fn pending_cell_retains_only_the_latest_waker() {
    let cell = PendingSendCell::new_for_test();
    let mut send = ProducerSend::from_pending(cell.clone());
    let replaced = CountingWake::new();
    let retained = CountingWake::new();
    assert_eq!(poll_send(&mut send, replaced.clone()), Poll::Pending);
    assert_eq!(poll_send(&mut send, retained.clone()), Poll::Pending);
    let promotion = cell
        .begin_promotion()
        .unwrap_or_else(|error| panic!("pending cell should claim: {error:?}"));
    let job = promotion
        .settle_local(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|error| panic!("local settlement should commit: {error:?}"));
    let mut registry = registry(1);
    notify(&registry, job);

    assert!(retained.wait_for_wake().is_some());
    assert_eq!(retained.count(), 1);
    assert_eq!(replaced.count(), 0);
    stop(&mut registry);
}

#[test]
fn blocking_wait_uses_the_same_pending_cell_and_local_result() {
    let cell = PendingSendCell::new_for_test();
    let send = ProducerSend::from_pending(cell.clone());
    let waiter = thread::spawn(move || send.wait());
    let promotion = cell
        .begin_promotion()
        .unwrap_or_else(|error| panic!("pending cell should claim: {error:?}"));
    let failure = ProducerSendFailure::new(ProducerSendFailureKind::Shutdown);
    let job = promotion
        .settle_local(failure)
        .unwrap_or_else(|error| panic!("local settlement should commit: {error:?}"));
    let mut registry = registry(1);
    notify(&registry, job);

    assert_eq!(
        waiter
            .join()
            .unwrap_or_else(|_panic| panic!("blocking waiter should return")),
        Err(ProducerSendError::Local(failure))
    );
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
    stop(&mut registry);
}

#[test]
fn bounded_notification_backpressure_returns_the_exact_pending_job() {
    let mut registry = registry(2);
    let (completion_id, mut completion) = registry
        .reserve()
        .unwrap_or_else(|error| panic!("completion should reserve: {error}"));
    let gate = GateWake::new();
    assert_eq!(
        poll_completion(&mut completion, gate.clone()),
        Poll::Pending
    );
    assert_eq!(
        registry.publish(
            completion_id,
            ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
                DeliveryStatus::NotSent,
            )),
        ),
        Ok(())
    );
    assert!(gate.wait_until_entered());

    let (mut first, first_wake, first_job) = pending_failure(ProducerSendFailureKind::Backpressure);
    let (mut second, second_wake, second_job) =
        pending_failure(ProducerSendFailureKind::DeadlineElapsed);
    let (mut retained, retained_wake, retained_job) =
        pending_failure(ProducerSendFailureKind::Shutdown);
    notify(&registry, first_job);
    notify(&registry, second_job);
    let retained_job = match registry.notify_pending(retained_job) {
        Err((crate::completion::CompletionRegistryError::NotificationBackpressure, job)) => job,
        Err((error, _job)) => panic!("unexpected pending notification failure: {error}"),
        Ok(()) => panic!("third pending job should remain caller-owned"),
    };

    gate.release();
    assert!(first_wake.wait_for_wake().is_some());
    notify(&registry, retained_job);
    assert!(second_wake.wait_for_wake().is_some());
    assert!(retained_wake.wait_for_wake().is_some());
    assert!(matches!(
        poll_send(&mut first, first_wake),
        Poll::Ready(Err(ProducerSendError::Local(_)))
    ));
    assert!(matches!(
        poll_send(&mut second, second_wake),
        Poll::Ready(Err(ProducerSendError::Local(_)))
    ));
    assert!(matches!(
        poll_send(&mut retained, retained_wake),
        Poll::Ready(Err(ProducerSendError::Local(_)))
    ));
    assert!(completion.wait().is_ok());
    wait_for_reclaim(&mut registry);
    stop(&mut registry);
}

fn registry(capacity: usize) -> CompletionRegistry<ProducerCompletion> {
    super::test_support::notification_registry(capacity)
}

fn pending_failure(
    kind: ProducerSendFailureKind,
) -> (
    ProducerSend,
    std::sync::Arc<CountingWake>,
    PendingNotificationJob,
) {
    let cell = PendingSendCell::new_for_test();
    let mut send = ProducerSend::from_pending(cell.clone());
    let wake = CountingWake::new();
    assert_eq!(poll_send(&mut send, wake.clone()), Poll::Pending);
    let promotion = cell
        .begin_promotion()
        .unwrap_or_else(|error| panic!("pending cell should claim: {error:?}"));
    let job = promotion
        .settle_local(ProducerSendFailure::new(kind))
        .unwrap_or_else(|error| panic!("local settlement should commit: {error:?}"));
    (send, wake, job)
}

fn wait_for_reclaim(registry: &mut CompletionRegistry<ProducerCompletion>) {
    for _attempt in 0..10_000 {
        match registry.next_reclaim() {
            Ok(Some(id)) => {
                assert_eq!(
                    registry.finish_reclaim(id),
                    Ok(crate::completion::ReclaimStatus::Reclaimed)
                );
                return;
            }
            Ok(None) => thread::yield_now(),
            Err(error) => panic!("completion reclaim should remain connected: {error}"),
        }
    }
    panic!("completion reclaim should become visible");
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
