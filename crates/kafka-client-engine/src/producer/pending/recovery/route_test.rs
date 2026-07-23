//! Constant-time route retention and exact terminal-owner scenarios.

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Wake, Waker},
};

use kafka_client_core::ProducerCompletion;

use super::{
    PendingNotificationPermitPool, PendingNotificationRoute, PendingNotificationRouteMode,
    PendingSendCell, ProducerSendFailure, ProducerSendFailureKind,
};
use crate::producer::pending::test_support::GateWake;
use crate::{ProducerSend, completion::CompletionRegistry};

#[test]
fn retain_stages_exact_fifo_without_running_application_wakers() {
    let pool = PendingNotificationPermitPool::new_for_test(2);
    let mut primary = CompletionRegistry::<ProducerCompletion>::new(2, 2)
        .unwrap_or_else(|error| panic!("primary notifier should start: {error}"));
    let mut route =
        PendingNotificationRoute::start(2).unwrap_or_else(|error| panic!("route start: {error}"));
    let order = Arc::new(Mutex::new(Vec::new()));
    let (first, first_send) = ordered_job(&pool, 1, &order);
    let (second, second_send) = ordered_job(&pool, 2, &order);

    assert_eq!(
        route
            .retain_pending_notification(first)
            .unwrap_or_else(|_failure| panic!("first job should retain")),
        PendingNotificationRouteMode::Primary
    );
    assert_eq!(
        route
            .retain_pending_notification(second)
            .unwrap_or_else(|_failure| panic!("second job should retain")),
        PendingNotificationRouteMode::Primary
    );
    assert_eq!(route.retained_len(), 2);
    assert!(lock(&order).is_empty());

    let primary_join = primary
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));
    let shutdown = route.begin_shutdown(primary_join);
    assert_eq!(
        shutdown.finish_notification_shutdown(),
        super::PendingNotificationShutdownFailures::default()
    );
    assert_eq!(*lock(&order), [1, 2]);
    assert_eq!(pool.in_use(), 0);
    drop((first_send, second_send));
}

#[test]
fn retain_overflow_returns_the_exact_newer_job() {
    let pool = PendingNotificationPermitPool::new_for_test(2);
    let mut primary = CompletionRegistry::<ProducerCompletion>::new(2, 2)
        .unwrap_or_else(|error| panic!("primary notifier should start: {error}"));
    let mut route =
        PendingNotificationRoute::start(1).unwrap_or_else(|error| panic!("route start: {error}"));
    let order = Arc::new(Mutex::new(Vec::new()));
    let (first, first_send) = ordered_job(&pool, 1, &order);
    let (second, second_send) = ordered_job(&pool, 2, &order);
    let expected = second.permit_slot_for_test();
    assert!(route.retain_pending_notification(first).is_ok());

    let returned = route
        .retain_pending_notification(second)
        .err()
        .unwrap_or_else(|| panic!("capacity disagreement must return the newer job"))
        .into_job();
    assert_eq!(returned.permit_slot_for_test(), expected);
    returned.dispatch_pending_notification_for_test();
    assert_eq!(*lock(&order), [2]);

    let primary_join = primary
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));
    let shutdown = route.begin_shutdown(primary_join);
    assert_eq!(
        shutdown.finish_notification_shutdown(),
        super::PendingNotificationShutdownFailures::default()
    );
    assert_eq!(*lock(&order), [2, 1]);
    assert_eq!(pool.in_use(), 0);
    drop((first_send, second_send));
}

#[test]
fn dropped_paired_owner_joins_primary_before_terminal_recovery() {
    let pool = PendingNotificationPermitPool::new_for_test(3);
    let mut primary = CompletionRegistry::<ProducerCompletion>::new(1, 1)
        .unwrap_or_else(|error| panic!("primary notifier should start: {error}"));
    let mut route =
        PendingNotificationRoute::start(3).unwrap_or_else(|error| panic!("route start: {error}"));
    let gate = GateWake::new();
    let (first, first_send) = pending_job(&pool, &Waker::from(gate.clone()));
    assert!(route.retain_pending_notification(first).is_ok());
    let _first = route.retry_primary_notifications(&primary, NonZeroUsize::MIN);
    assert!(gate.wait_until_entered());

    let order = Arc::new(Mutex::new(Vec::new()));
    let (second, second_send) = ordered_job(&pool, 2, &order);
    let (third, third_send) = ordered_job(&pool, 3, &order);
    assert!(route.retain_pending_notification(second).is_ok());
    assert!(route.retain_pending_notification(third).is_ok());
    let primary_join = primary
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));
    let closed = route.retry_primary_notifications(&primary, NonZeroUsize::MIN);
    assert_eq!(closed.mode(), PendingNotificationRouteMode::Recovery);

    let notifications = route.begin_shutdown(primary_join);
    let (done_sender, done_receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        drop(notifications);
        done_sender
            .send(())
            .unwrap_or_else(|_error| panic!("paired-owner drop should finish"));
    });
    assert!(lock(&order).is_empty());
    assert!(matches!(
        done_receiver.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));

    gate.release();
    done_receiver
        .recv()
        .unwrap_or_else(|error| panic!("paired-owner drop should join both workers: {error}"));
    assert_eq!(*lock(&order), [2, 3]);
    assert_eq!(pool.in_use(), 0);
    drop((first_send, second_send, third_send));
}

#[test]
fn missing_primary_refuses_retained_tail_without_splitting_owner() {
    let pool = PendingNotificationPermitPool::new_for_test(1);
    let mut primary = CompletionRegistry::<ProducerCompletion>::new(1, 1)
        .unwrap_or_else(|error| panic!("primary notifier should start: {error}"));
    let mut route =
        PendingNotificationRoute::start(1).unwrap_or_else(|error| panic!("route start: {error}"));
    let order = Arc::new(Mutex::new(Vec::new()));
    let (job, send) = ordered_job(&pool, 1, &order);
    assert!(route.retain_pending_notification(job).is_ok());

    let refusal = route
        .begin_empty_recovery_without_primary()
        .err()
        .unwrap_or_else(|| panic!("retained work must require primary ownership"));
    assert_eq!(refusal.retained_jobs, 1);
    assert!(lock(&order).is_empty());

    let primary_join = primary
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));
    let shutdown = route.begin_shutdown(primary_join);
    assert_eq!(
        shutdown.finish_notification_shutdown(),
        super::PendingNotificationShutdownFailures::default()
    );
    assert_eq!(*lock(&order), [1]);
    assert_eq!(pool.in_use(), 0);
    drop(send);
}

fn pending_job(
    pool: &Arc<PendingNotificationPermitPool>,
    waker: &Waker,
) -> (super::PendingNotificationJob, ProducerSend) {
    let permit = pool
        .reserve()
        .unwrap_or_else(|| panic!("test pending permit should reserve"));
    let cell = PendingSendCell::new(permit);
    let mut send = ProducerSend::from_pending(cell.clone());
    let mut context = Context::from_waker(waker);
    assert_eq!(Pin::new(&mut send).poll(&mut context), Poll::Pending);
    let job = cell
        .settle_local_for_test(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|error| panic!("pending settlement should commit: {error:?}"));
    (job, send)
}

fn ordered_job(
    pool: &Arc<PendingNotificationPermitPool>,
    value: usize,
    order: &Arc<Mutex<Vec<usize>>>,
) -> (super::PendingNotificationJob, ProducerSend) {
    let waker = Waker::from(Arc::new(OrderWake {
        value,
        order: Arc::clone(order),
    }));
    pending_job(pool, &waker)
}

struct OrderWake {
    value: usize,
    order: Arc<Mutex<Vec<usize>>>,
}

impl Wake for OrderWake {
    fn wake(self: Arc<Self>) {
        lock(&self.order).push(self.value);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
