//! Pending notification ownership at the bounded registry boundary.

use kafka_client_core::ProducerCompletion;

use crate::{
    ProducerSendError, ProducerSendFailure, ProducerSendFailureKind,
    completion::{CompletionRegistry, CompletionRegistryError},
    producer::pending::{
        PendingSendCell,
        test_support::{CountingWake, poll_send},
    },
};

#[test]
fn stopped_notifier_returns_the_exact_pending_cell_job() {
    let caller = std::thread::current().id();
    let mut registry = CompletionRegistry::<ProducerCompletion>::new(1, 1)
        .unwrap_or_else(|error| panic!("completion notifier should start: {error}"));
    let join = registry
        .stop_notifier()
        .unwrap_or_else(|error| panic!("empty completion notifier should stop: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
    let cell = PendingSendCell::new_for_test();
    let mut send = crate::ProducerSend::from_pending(cell.clone());
    let wake = CountingWake::new();
    let _pending = poll_send(&mut send, wake.clone());
    let failure = ProducerSendFailure::new(ProducerSendFailureKind::Shutdown);
    let job = cell
        .settle_local_for_test(failure)
        .unwrap_or_else(|error| panic!("local settlement should commit: {error:?}"));

    let returned = match registry.notify_pending(job) {
        Err((CompletionRegistryError::NotifierStopped, job)) => job,
        Err((error, _job)) => panic!("unexpected notifier failure: {error}"),
        Ok(()) => panic!("stopped notifier must return pending ownership"),
    };
    let dispatcher = std::thread::spawn(move || returned.dispatch_pending_notification_for_test());
    let Some(wake_thread) = wake.wait_for_wake() else {
        panic!("off-reactor recovery should wake the pending operation");
    };
    assert_ne!(wake_thread, caller);
    dispatcher
        .join()
        .unwrap_or_else(|_panic| panic!("off-reactor recovery should return"));
    assert_eq!(
        poll_send(&mut send, wake),
        std::task::Poll::Ready(Err(ProducerSendError::Local(failure)))
    );
}

#[test]
fn notifier_stop_drains_an_already_queued_pending_job() {
    let caller = std::thread::current().id();
    let mut registry = CompletionRegistry::<ProducerCompletion>::new(1, 1)
        .unwrap_or_else(|error| panic!("completion notifier should start: {error}"));
    let notifier = registry
        .notifier_thread_id()
        .unwrap_or_else(|| panic!("notifier identity should exist"));
    let cell = PendingSendCell::new_for_test();
    let mut send = crate::ProducerSend::from_pending(cell.clone());
    let wake = CountingWake::new();
    let _pending = poll_send(&mut send, wake.clone());
    let failure = ProducerSendFailure::new(ProducerSendFailureKind::Closed);
    let job = cell
        .settle_local_for_test(failure)
        .unwrap_or_else(|error| panic!("local settlement should commit: {error:?}"));
    if let Err((error, _job)) = registry.notify_pending(job) {
        panic!("pending notification should queue before stop: {error}");
    }

    let join = registry
        .stop_notifier()
        .unwrap_or_else(|error| panic!("notifier stop should accept queued pending work: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
    let wake_thread = wake
        .wait_for_wake()
        .unwrap_or_else(|| panic!("queued pending job should wake before notifier exit"));
    assert_eq!(wake_thread, notifier);
    assert_ne!(wake_thread, caller);
    assert_eq!(
        poll_send(&mut send, wake),
        std::task::Poll::Ready(Err(ProducerSendError::Local(failure)))
    );
}
