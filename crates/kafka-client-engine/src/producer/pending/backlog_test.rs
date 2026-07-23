//! Exact FIFO retention and closed-notifier recovery ownership scenarios.

use std::task::Poll;

use super::{
    PendingNotificationBacklog, PendingNotificationPermitPool, PendingSendCell,
    ProducerSendFailure, ProducerSendFailureKind,
    test_support::{CountingWake, poll_send},
};
use crate::{ProducerSendError, producer::boundary::ProducerSend};

#[test]
fn backlog_retains_exact_jobs_in_fifo_order() {
    let pool = PendingNotificationPermitPool::new_for_test(3);
    let (first, mut first_send) = pending_job(&pool, ProducerSendFailureKind::DeadlineElapsed);
    let (second, mut second_send) = pending_job(&pool, ProducerSendFailureKind::Backpressure);
    let (returned, mut returned_send) = pending_job(&pool, ProducerSendFailureKind::Shutdown);
    let expected = [
        first.permit_slot_for_test(),
        second.permit_slot_for_test(),
        returned.permit_slot_for_test(),
    ];
    let mut backlog = PendingNotificationBacklog::new(2);
    assert!(backlog.try_push(first).is_ok());
    assert!(backlog.try_push(second).is_ok());
    let returned = backlog
        .try_push(returned)
        .err()
        .unwrap_or_else(|| panic!("full backlog should return the exact job"))
        .into_job();
    assert_eq!(backlog.len(), 2);

    let recovery = backlog.into_recovery(returned);
    assert_eq!(recovery.permit_order_for_test(), expected);
    let dispatcher =
        std::thread::spawn(move || recovery.dispatch_all_pending_notifications_for_test());
    dispatcher
        .join()
        .unwrap_or_else(|_panic| panic!("off-reactor recovery should return"));
    assert_local(&mut first_send, ProducerSendFailureKind::DeadlineElapsed);
    assert_local(&mut second_send, ProducerSendFailureKind::Backpressure);
    assert_local(&mut returned_send, ProducerSendFailureKind::Shutdown);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn closed_transfer_keeps_backlog_and_returned_job_in_one_recovery_owner() {
    let pool = PendingNotificationPermitPool::new_for_test(2);
    let (queued, mut queued_send) = pending_job(&pool, ProducerSendFailureKind::Closed);
    let (returned, mut returned_send) = pending_job(&pool, ProducerSendFailureKind::Shutdown);
    let mut backlog = PendingNotificationBacklog::new(1);
    assert!(backlog.try_push(queued).is_ok());
    let recovery = backlog.into_recovery(returned);

    let dispatcher =
        std::thread::spawn(move || recovery.dispatch_all_pending_notifications_for_test());
    dispatcher
        .join()
        .unwrap_or_else(|_panic| panic!("off-reactor recovery should return"));
    assert_local(&mut queued_send, ProducerSendFailureKind::Closed);
    assert_local(&mut returned_send, ProducerSendFailureKind::Shutdown);
    assert_eq!(pool.in_use(), 0);
}

fn pending_job(
    pool: &std::sync::Arc<PendingNotificationPermitPool>,
    kind: ProducerSendFailureKind,
) -> (super::PendingNotificationJob, ProducerSend) {
    let permit = pool
        .reserve()
        .unwrap_or_else(|| panic!("test pending permit should reserve"));
    let cell = PendingSendCell::new(permit);
    let send = ProducerSend::from_pending(cell.clone());
    let promotion = cell
        .begin_promotion()
        .unwrap_or_else(|error| panic!("pending cell should claim: {error:?}"));
    let job = promotion
        .settle_local(ProducerSendFailure::new(kind))
        .unwrap_or_else(|(_promotion, error)| panic!("local settlement should commit: {error:?}"));
    (job, send)
}

fn assert_local(send: &mut ProducerSend, expected: ProducerSendFailureKind) {
    let wake = CountingWake::new();
    let Poll::Ready(Err(ProducerSendError::Local(failure))) = poll_send(send, wake) else {
        panic!("recovered pending send should expose its local failure");
    };
    assert_eq!(failure.kind(), expected);
}
