//! Recovery queue capacity and exact-failure ownership scenarios.

use super::{
    PendingNotificationBacklog, PendingNotificationPermitPool, PendingRecoveryQueue,
    PendingRecoverySubmitErrorKind, PendingSendCell, ProducerSendFailure, ProducerSendFailureKind,
};

#[test]
fn full_and_stopped_submissions_return_the_exact_fifo_batch() {
    let pool = PendingNotificationPermitPool::new_for_test(3);
    let first = pending_job(&pool);
    let second = pending_job(&pool);
    let third = pending_job(&pool);
    let expected_second = second.permit_slot_for_test();
    let expected_third = third.permit_slot_for_test();
    let queue = PendingRecoveryQueue::new(1);

    assert!(queue.try_submit(recovery(first)).is_ok());
    let full = queue
        .try_submit(recovery(second))
        .err()
        .unwrap_or_else(|| panic!("fixed recovery queue should report full"));
    assert_eq!(full.kind(), PendingRecoverySubmitErrorKind::Full);
    assert_eq!(
        full.into_recovery().permit_order_for_test(),
        [expected_second]
    );

    queue.close();
    let stopped = queue
        .try_submit(recovery(third))
        .err()
        .unwrap_or_else(|| panic!("closed recovery queue should return its batch"));
    assert_eq!(stopped.kind(), PendingRecoverySubmitErrorKind::Stopped);
    assert_eq!(
        stopped.into_recovery().permit_order_for_test(),
        [expected_third]
    );

    let queued = queue
        .next()
        .unwrap_or_else(|| panic!("the first exact batch should remain queued"));
    queued.dispatch_all_pending_notifications_for_test();
    assert!(queue.next().is_none());
}

#[test]
fn permit_capacity_bounds_queued_and_current_recovery_jobs_together() {
    let pool = PendingNotificationPermitPool::new_for_test(2);
    let first = pending_job(&pool);
    let second = pending_job(&pool);
    assert!(pool.reserve().is_none());
    let queue = PendingRecoveryQueue::new(pool.capacity());

    assert!(queue.try_submit(recovery(first)).is_ok());
    let current = queue
        .next()
        .unwrap_or_else(|| panic!("worker should own the oldest recovery"));
    assert!(queue.try_submit(recovery(second)).is_ok());
    assert!(pool.reserve().is_none());

    current.dispatch_all_pending_notifications_for_test();
    queue.close();
    queue
        .next()
        .unwrap_or_else(|| panic!("newer recovery should remain queued"))
        .dispatch_all_pending_notifications_for_test();
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn terminal_tail_remains_fifo_when_the_normal_queue_is_full() {
    let pool = PendingNotificationPermitPool::new_for_test(2);
    let first = pending_job(&pool);
    let second = pending_job(&pool);
    let first_slot = first.permit_slot_for_test();
    let second_slot = second.permit_slot_for_test();
    let queue = PendingRecoveryQueue::new(1);

    assert!(queue.try_submit(recovery(first)).is_ok());
    queue.close_with_terminal(Some(recovery(second)));

    let first = queue
        .next()
        .unwrap_or_else(|| panic!("normal FIFO head should remain"));
    assert_eq!(first.permit_order_for_test(), [first_slot]);
    first.dispatch_all_pending_notifications_for_test();
    let second = queue
        .next()
        .unwrap_or_else(|| panic!("terminal FIFO tail should remain"));
    assert_eq!(second.permit_order_for_test(), [second_slot]);
    second.dispatch_all_pending_notifications_for_test();
    assert!(queue.next().is_none());
}

fn recovery(job: super::PendingNotificationJob) -> super::PendingNotificationRecovery {
    PendingNotificationBacklog::new(0).into_recovery(job)
}

fn pending_job(
    pool: &std::sync::Arc<PendingNotificationPermitPool>,
) -> super::PendingNotificationJob {
    let permit = pool
        .reserve()
        .unwrap_or_else(|| panic!("test pending permit should reserve"));
    let cell = PendingSendCell::new(permit);
    cell.settle_local_for_test(ProducerSendFailure::new(
        ProducerSendFailureKind::Backpressure,
    ))
    .unwrap_or_else(|error| panic!("pending local settlement should commit: {error:?}"))
}
