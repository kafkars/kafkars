//! Shared FIFO budget and exact typed-job ownership scenarios.

use std::sync::{Arc, mpsc::sync_channel};

use super::{
    cell::CompletionCell,
    notifier::PublishJob,
    notifier_queue::{NotificationJob, NotificationQueue, QueuePushError},
};
use crate::{
    ProducerSendError, ProducerSendFailure, ProducerSendFailureKind,
    producer::pending::{
        PendingSendCell,
        test_support::{CountingWake, poll_send},
    },
};

#[test]
fn pending_queue_backpressure_returns_the_exact_live_job() {
    let queue = NotificationQueue::<u8>::new_for_test(1);
    let (first, first_send) = pending_job();
    let (retained, retained_send) = pending_job();
    assert!(queue.try_pending(first).is_ok());
    let retained = match queue.try_pending(retained) {
        Err(QueuePushError::Full(job)) => job,
        Err(QueuePushError::Closed(_job)) => panic!("open queue should report capacity"),
        Ok(()) => panic!("bounded queue should retain the second job"),
    };

    let Some(NotificationJob::Pending(first)) = queue.next() else {
        panic!("first pending job should remain queued");
    };
    first.dispatch_pending_notification_for_test();
    assert!(queue.try_pending(retained).is_ok());
    queue.close();
    let Some(NotificationJob::Pending(retained)) = queue.next() else {
        panic!("retained pending job should queue unchanged");
    };
    retained.dispatch_pending_notification_for_test();
    assert!(queue.next().is_none());
    drop(first_send);
    drop(retained_send);
}

#[test]
fn terminal_and_pending_jobs_share_one_fifo_order() {
    let queue = NotificationQueue::<u8>::new_for_test(2);
    let (pending, mut send) = pending_job();
    assert!(queue.try_pending(pending).is_ok());
    assert!(queue.try_publish(publish_job(17)).is_ok());

    let Some(NotificationJob::Pending(pending)) = queue.next() else {
        panic!("pending job should remain first in the shared FIFO");
    };
    pending.dispatch_pending_notification_for_test();
    assert!(matches!(
        poll_send(&mut send, CountingWake::new()),
        std::task::Poll::Ready(Err(ProducerSendError::Local(_)))
    ));
    let Some(NotificationJob::Publish(publish)) = queue.next() else {
        panic!("terminal job should remain second in the shared FIFO");
    };
    assert_eq!(publish.value, 17);
    queue.close();
    assert!(queue.next().is_none());
}

#[test]
fn terminal_job_uses_capacity_needed_by_a_pending_job() {
    let queue = NotificationQueue::<u8>::new_for_test(1);
    assert!(queue.try_publish(publish_job(23)).is_ok());
    let (pending, mut send) = pending_job();
    let pending = match queue.try_pending(pending) {
        Err(QueuePushError::Full(job)) => job,
        Err(QueuePushError::Closed(_job)) => panic!("open queue should report capacity"),
        Ok(()) => panic!("terminal job should consume the shared capacity"),
    };

    let Some(NotificationJob::Publish(publish)) = queue.next() else {
        panic!("terminal job should remain queued");
    };
    assert_eq!(publish.value, 23);
    pending.dispatch_pending_notification_for_test();
    assert!(matches!(
        poll_send(&mut send, CountingWake::new()),
        std::task::Poll::Ready(Err(ProducerSendError::Local(_)))
    ));
    queue.close();
    assert!(queue.next().is_none());
}

fn pending_job() -> (
    crate::producer::pending::PendingNotificationJob,
    crate::ProducerSend,
) {
    let cell = PendingSendCell::new_for_test();
    let mut send = crate::ProducerSend::from_pending(cell.clone());
    let _pending =
        crate::producer::pending::test_support::poll_send(&mut send, CountingWake::new());
    let job = cell
        .settle_local_for_test(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|error| panic!("local settlement should commit: {error:?}"));
    (job, send)
}

fn publish_job(value: u8) -> PublishJob<u8> {
    let (reclaim, _reclaims) = sync_channel(1);
    let cell = Arc::new(CompletionCell::new(0, reclaim));
    let id = cell
        .activate()
        .unwrap_or_else(|error| panic!("test completion should activate: {error}"));
    PublishJob { id, cell, value }
}
