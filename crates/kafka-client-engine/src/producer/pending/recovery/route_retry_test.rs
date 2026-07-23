//! Explicit retry-budget, primary backpressure, and closure-order scenarios.

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
    PendingNotificationRouteProgress, PendingSendCell, ProducerSendFailure,
    ProducerSendFailureKind,
};
use crate::{
    ProducerSend, completion::CompletionRegistry, producer::pending::test_support::GateWake,
};

#[test]
fn retry_respects_explicit_limit_and_reports_runnable_tail() {
    let pool = PendingNotificationPermitPool::new_for_test(3);
    let mut primary = CompletionRegistry::<ProducerCompletion>::new(3, 3)
        .unwrap_or_else(|error| panic!("primary notifier should start: {error}"));
    let mut route =
        PendingNotificationRoute::start(3).unwrap_or_else(|error| panic!("route start: {error}"));
    let order = Arc::new(Mutex::new(Vec::new()));
    let (first, first_send) = ordered_job(&pool, 1, &order);
    let (second, second_send) = ordered_job(&pool, 2, &order);
    let (third, third_send) = ordered_job(&pool, 3, &order);
    assert!(route.retain_pending_notification(first).is_ok());
    assert!(route.retain_pending_notification(second).is_ok());
    assert!(route.retain_pending_notification(third).is_ok());

    let first: PendingNotificationRouteProgress =
        route.retry_primary_notifications(&primary, NonZeroUsize::MIN);
    assert_eq!(first.attempted(), 1);
    assert!(first.remaining());
    assert!(!first.blocked());
    assert_eq!(first.mode(), PendingNotificationRouteMode::Primary);

    let rest = route.retry_primary_notifications(&primary, nonzero(2));
    assert_eq!(rest.attempted(), 2);
    assert!(!rest.remaining());
    assert!(!rest.blocked());
    assert_eq!(rest.mode(), PendingNotificationRouteMode::Primary);

    let primary_join = primary
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));
    let shutdown = route.begin_shutdown(primary_join);
    assert_eq!(
        shutdown.finish_notification_shutdown(),
        super::PendingNotificationShutdownFailures::default()
    );
    assert_eq!(*lock(&order), [1, 2, 3]);
    assert_eq!(pool.in_use(), 0);
    drop((first_send, second_send, third_send));
}

#[test]
fn backpressured_head_stays_before_every_newer_retained_job() {
    let pool = PendingNotificationPermitPool::new_for_test(4);
    let mut primary = CompletionRegistry::<ProducerCompletion>::new(1, 1)
        .unwrap_or_else(|error| panic!("primary notifier should start: {error}"));
    let mut route =
        PendingNotificationRoute::start(4).unwrap_or_else(|error| panic!("route start: {error}"));
    let gate = GateWake::new();
    let (first, first_send) = pending_job(&pool, &Waker::from(gate.clone()));
    assert!(route.retain_pending_notification(first).is_ok());
    assert_eq!(
        route
            .retry_primary_notifications(&primary, NonZeroUsize::MIN)
            .attempted(),
        1
    );
    assert!(gate.wait_until_entered());

    let order = Arc::new(Mutex::new(Vec::new()));
    let (second, second_send) = ordered_job(&pool, 2, &order);
    let (third, third_send) = ordered_job(&pool, 3, &order);
    let (fourth, fourth_send) = ordered_job(&pool, 4, &order);
    assert!(route.retain_pending_notification(second).is_ok());
    assert!(route.retain_pending_notification(third).is_ok());
    let blocked = route.retry_primary_notifications(&primary, nonzero(2));
    assert_eq!(blocked.attempted(), 2);
    assert!(blocked.remaining());
    assert!(blocked.blocked());
    assert_eq!(blocked.mode(), PendingNotificationRouteMode::Primary);
    assert!(route.retain_pending_notification(fourth).is_ok());

    gate.release();
    wait_for_order(&order, 1);
    retry_until_submitted(&mut route, &primary);
    wait_for_order(&order, 2);
    retry_until_submitted(&mut route, &primary);
    assert_eq!(route.retained_len(), 0);
    let primary_join = primary
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));
    let shutdown = route.begin_shutdown(primary_join);
    assert_eq!(
        shutdown.finish_notification_shutdown(),
        super::PendingNotificationShutdownFailures::default()
    );
    assert_eq!(*lock(&order), [2, 3, 4]);
    assert_eq!(pool.in_use(), 0);
    drop((first_send, second_send, third_send, fourth_send));
}

#[test]
fn primary_closure_moves_fifo_to_blocked_recovery_until_primary_join() {
    let pool = PendingNotificationPermitPool::new_for_test(4);
    let mut primary = CompletionRegistry::<ProducerCompletion>::new(1, 1)
        .unwrap_or_else(|error| panic!("primary notifier should start: {error}"));
    let mut route =
        PendingNotificationRoute::start(4).unwrap_or_else(|error| panic!("route start: {error}"));
    let gate = GateWake::new();
    let (first, first_send) = pending_job(&pool, &Waker::from(gate.clone()));
    assert!(route.retain_pending_notification(first).is_ok());
    let _first = route.retry_primary_notifications(&primary, NonZeroUsize::MIN);
    assert!(gate.wait_until_entered());

    let order = Arc::new(Mutex::new(Vec::new()));
    let (second, second_send) = ordered_job(&pool, 2, &order);
    let (third, third_send) = ordered_job(&pool, 3, &order);
    let (fourth, fourth_send) = ordered_job(&pool, 4, &order);
    assert!(route.retain_pending_notification(second).is_ok());
    assert!(route.retain_pending_notification(third).is_ok());
    let primary_join = primary
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));

    let closed = route.retry_primary_notifications(&primary, NonZeroUsize::MIN);
    assert_eq!(closed.attempted(), 1);
    assert!(closed.remaining());
    assert!(closed.blocked());
    assert_eq!(closed.mode(), PendingNotificationRouteMode::Recovery);
    assert_eq!(
        route
            .retain_pending_notification(fourth)
            .unwrap_or_else(|_failure| panic!("later recovery job should retain")),
        PendingNotificationRouteMode::Recovery
    );
    let still_blocked = route.retry_primary_notifications(&primary, nonzero(4));
    assert_eq!(still_blocked.attempted(), 0);
    assert!(still_blocked.remaining());
    assert!(still_blocked.blocked());
    assert!(lock(&order).is_empty());

    let shutdown = route.begin_shutdown(primary_join);
    let (done_sender, done_receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let failures = shutdown.finish_notification_shutdown();
        done_sender
            .send(failures)
            .unwrap_or_else(|_error| panic!("shutdown result should transfer"));
    });
    assert!(lock(&order).is_empty());
    assert!(matches!(
        done_receiver.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    gate.release();
    assert_eq!(
        done_receiver
            .recv()
            .unwrap_or_else(|error| panic!("notification shutdown should finish: {error}")),
        super::PendingNotificationShutdownFailures::default()
    );
    assert_eq!(*lock(&order), [2, 3, 4]);
    assert_eq!(pool.in_use(), 0);
    drop((first_send, second_send, third_send, fourth_send));
}

fn ordered_job(
    pool: &Arc<PendingNotificationPermitPool>,
    value: usize,
    order: &Arc<Mutex<Vec<usize>>>,
) -> (super::PendingNotificationJob, ProducerSend) {
    pending_job(
        pool,
        &Waker::from(Arc::new(OrderWake {
            value,
            order: Arc::clone(order),
        })),
    )
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

struct OrderWake {
    value: usize,
    order: Arc<Mutex<Vec<usize>>>,
}

impl Wake for OrderWake {
    fn wake(self: Arc<Self>) {
        lock(&self.order).push(self.value);
    }
}

fn wait_for_order(order: &Mutex<Vec<usize>>, expected: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while lock(order).len() < expected {
        assert!(
            std::time::Instant::now() < deadline,
            "primary notifier should preserve pending FIFO"
        );
        std::thread::yield_now();
    }
}

fn retry_until_submitted(
    route: &mut PendingNotificationRoute,
    primary: &CompletionRegistry<ProducerCompletion>,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let progress = route.retry_primary_notifications(primary, NonZeroUsize::MIN);
        if !progress.blocked() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "primary notifier should eventually accept the retained head"
        );
        std::thread::yield_now();
    }
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
