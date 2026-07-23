//! Pending-send notification isolation from host and driver reactor threads.

use std::{task::Poll, thread};

use kafka_client_core::ProducerCompletion;

use super::{
    PendingSendCell, ProducerSendFailure, ProducerSendFailureKind,
    test_support::{CountingWake, poll_send},
};
use crate::{ProducerSendError, completion::CompletionRegistry, producer::boundary::ProducerSend};

#[test]
fn local_failure_wakes_only_on_the_completion_notifier() {
    let caller = thread::current().id();
    let cell = PendingSendCell::new_for_test();
    let mut send = ProducerSend::from_pending(cell.clone());
    let wake = CountingWake::new();
    assert_eq!(poll_send(&mut send, wake.clone()), Poll::Pending);
    let promotion = cell
        .begin_promotion()
        .unwrap_or_else(|error| panic!("pending cell should claim: {error:?}"));
    let failure = ProducerSendFailure::new(ProducerSendFailureKind::DeadlineElapsed);
    let job = promotion
        .settle_local(failure)
        .unwrap_or_else(|error| panic!("local settlement should commit: {error:?}"));
    let mut registry = CompletionRegistry::<ProducerCompletion>::new(1, 1)
        .unwrap_or_else(|error| panic!("completion notifier should start: {error}"));

    if let Err((error, _job)) = registry.notify_pending(job) {
        panic!("pending notification should queue: {error}");
    }
    let wake_thread = wake
        .wait_for_wake()
        .unwrap_or_else(|| panic!("pending notifier should wake the operation"));
    assert_ne!(wake_thread, caller);
    assert_eq!(
        poll_send(&mut send, wake),
        Poll::Ready(Err(ProducerSendError::Local(failure)))
    );
    let join = registry
        .stop_notifier()
        .unwrap_or_else(|error| panic!("completion notifier should stop: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}
