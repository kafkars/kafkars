//! Terminal notifier FIFO capacity, closure, and exact-owner scenarios.

use std::sync::{Arc, mpsc::sync_channel};

use super::{
    cell::CompletionCell,
    notifier::PublishJob,
    notifier_queue::{NotificationQueue, QueuePushError},
};

#[test]
fn close_drains_terminal_jobs_in_fifo_order() {
    let queue = NotificationQueue::new(2);
    assert!(queue.try_publish(publish_job(0, 11)).is_ok());
    assert!(queue.try_publish(publish_job(1, 12)).is_ok());
    queue.close();

    let Some(first) = queue.next() else {
        panic!("first terminal job should remain queued");
    };
    let Some(second) = queue.next() else {
        panic!("second terminal job should remain queued");
    };

    assert_eq!(first.value, 11);
    assert_eq!(second.value, 12);
    assert!(queue.next().is_none());
}

#[test]
fn full_queue_returns_the_exact_terminal_job() {
    let queue = NotificationQueue::new(1);
    assert!(queue.try_publish(publish_job(0, 21)).is_ok());
    let expected = publish_job(1, 22);
    let expected_id = expected.id;
    let expected_cell = Arc::clone(&expected.cell);

    let returned = match queue.try_publish(expected) {
        Err(QueuePushError::Full(job)) => job,
        Err(QueuePushError::Closed(_job)) => panic!("open queue should report capacity"),
        Ok(()) => panic!("bounded queue should return the newer terminal job"),
    };

    assert_eq!(returned.id, expected_id);
    assert!(Arc::ptr_eq(&returned.cell, &expected_cell));
    assert_eq!(returned.value, 22);
    queue.close();
    assert!(queue.next().is_some());
    assert!(queue.next().is_none());
}

#[test]
fn closed_queue_returns_the_exact_terminal_job() {
    let queue = NotificationQueue::new(1);
    queue.close();
    let expected = publish_job(0, 31);
    let expected_id = expected.id;
    let expected_cell = Arc::clone(&expected.cell);

    let returned = match queue.try_publish(expected) {
        Err(QueuePushError::Closed(job)) => job,
        Err(QueuePushError::Full(_job)) => panic!("closed queue should report closure"),
        Ok(()) => panic!("closed queue cannot accept a terminal job"),
    };

    assert_eq!(returned.id, expected_id);
    assert!(Arc::ptr_eq(&returned.cell, &expected_cell));
    assert_eq!(returned.value, 31);
    assert!(queue.next().is_none());
}

fn publish_job(slot: usize, value: u8) -> PublishJob<u8> {
    let (reclaim, _reclaims) = sync_channel(1);
    let cell = Arc::new(CompletionCell::new(slot, reclaim));
    let id = cell
        .activate()
        .unwrap_or_else(|error| panic!("test completion should activate: {error}"));
    PublishJob { id, cell, value }
}
