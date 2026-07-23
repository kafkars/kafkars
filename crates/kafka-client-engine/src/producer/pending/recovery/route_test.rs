//! Primary retry, irreversible recovery, and cross-route FIFO scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Wake, Waker},
};

use kafka_client_core::ProducerCompletion;

use super::{
    PendingNotificationPermitPool, PendingNotificationRoute, PendingNotificationRouteMode,
    PendingSendCell, ProducerSendFailure, ProducerSendFailureKind,
};
use crate::{
    ProducerSend, completion::CompletionRegistry, producer::pending::test_support::GateWake,
};

#[test]
fn front_retry_closure_transfers_fifo_then_routes_later_jobs_to_recovery() {
    let pool = PendingNotificationPermitPool::new_for_test(6);
    let mut primary = CompletionRegistry::<ProducerCompletion>::new(1, 1)
        .unwrap_or_else(|error| panic!("primary notifier should start: {error}"));
    let mut route =
        PendingNotificationRoute::start(6).unwrap_or_else(|error| panic!("route start: {error}"));
    let order = Arc::new(Mutex::new(Vec::new()));
    let gate = GateWake::new();
    let (first, first_send) = pending_job(&pool, &Waker::from(gate.clone()));

    assert_eq!(
        route
            .notify(&primary, first)
            .unwrap_or_else(|_failure| panic!("first job should enter primary")),
        PendingNotificationRouteMode::Primary
    );
    assert!(gate.wait_until_entered());

    let (second, second_send) = ordered_job(&pool, 2, &order);
    let (third, third_send) = ordered_job(&pool, 3, &order);
    let (fourth, fourth_send) = ordered_job(&pool, 4, &order);
    assert_eq!(
        route
            .notify(&primary, second)
            .unwrap_or_else(|_failure| panic!("second job should queue")),
        PendingNotificationRouteMode::Primary
    );
    assert_eq!(
        route
            .notify(&primary, third)
            .unwrap_or_else(|_failure| panic!("third job should backlog")),
        PendingNotificationRouteMode::Primary
    );
    assert_eq!(
        route
            .notify(&primary, fourth)
            .unwrap_or_else(|_failure| panic!("fourth job should follow third")),
        PendingNotificationRouteMode::Primary
    );
    assert_eq!(route.retained_len(), 2);

    let primary_join = primary
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));
    gate.release();

    let (fifth, fifth_send) = ordered_job(&pool, 5, &order);
    assert_eq!(
        route
            .notify(&primary, fifth)
            .unwrap_or_else(|_failure| panic!("closed primary should transfer")),
        PendingNotificationRouteMode::Recovery
    );
    let (sixth, sixth_send) = ordered_job(&pool, 6, &order);
    assert_eq!(
        route
            .notify(&primary, sixth)
            .unwrap_or_else(|_failure| panic!("later job should use recovery")),
        PendingNotificationRouteMode::Recovery
    );
    assert!(lock(&order).iter().all(|value| *value == 2));
    let shutdown = route.begin_shutdown(primary_join);
    assert_eq!(
        shutdown.finish_notification_shutdown(),
        super::PendingNotificationShutdownFailures::default()
    );
    wait_for_order(&order, 5);
    assert_eq!(*lock(&order), [2, 3, 4, 5, 6]);
    assert_eq!(pool.in_use(), 0);
    drop((
        first_send,
        second_send,
        third_send,
        fourth_send,
        fifth_send,
        sixth_send,
    ));
}

#[test]
fn impossible_backlog_overflow_returns_the_exact_newer_job() {
    let pool = PendingNotificationPermitPool::new_for_test(4);
    let mut primary = CompletionRegistry::<ProducerCompletion>::new(1, 1)
        .unwrap_or_else(|error| panic!("primary notifier should start: {error}"));
    let mut route =
        PendingNotificationRoute::start(1).unwrap_or_else(|error| panic!("route start: {error}"));
    let gate = GateWake::new();
    let (first, first_send) = pending_job(&pool, &Waker::from(gate.clone()));
    assert!(route.notify(&primary, first).is_ok());
    assert!(gate.wait_until_entered());
    let order = Arc::new(Mutex::new(Vec::new()));
    let (second, second_send) = ordered_job(&pool, 2, &order);
    let (third, third_send) = ordered_job(&pool, 3, &order);
    let (fourth, fourth_send) = ordered_job(&pool, 4, &order);
    assert!(route.notify(&primary, second).is_ok());
    assert!(route.notify(&primary, third).is_ok());

    let returned = route
        .notify(&primary, fourth)
        .err()
        .unwrap_or_else(|| panic!("capacity disagreement must return the newer job"))
        .into_job();
    returned.dispatch_pending_notification_for_test();
    assert_eq!(*lock(&order), [4]);

    let primary_join = primary
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));
    gate.release();
    let shutdown = route.begin_shutdown(primary_join);
    assert_eq!(
        shutdown.finish_notification_shutdown(),
        super::PendingNotificationShutdownFailures::default()
    );
    wait_for_order(&order, 3);
    assert_eq!(*lock(&order), [4, 2, 3]);
    assert_eq!(pool.in_use(), 0);
    drop((first_send, second_send, third_send, fourth_send));
}

#[test]
fn dropped_paired_owner_joins_primary_before_terminal_recovery() {
    let pool = PendingNotificationPermitPool::new_for_test(3);
    let mut primary = CompletionRegistry::<ProducerCompletion>::new(1, 1)
        .unwrap_or_else(|error| panic!("primary notifier should start: {error}"));
    let mut route =
        PendingNotificationRoute::start(3).unwrap_or_else(|error| panic!("route start: {error}"));
    let order = Arc::new(Mutex::new(Vec::new()));
    let gate = GateWake::new();
    let (first, first_send) = pending_job(&pool, &Waker::from(gate.clone()));
    assert!(route.notify(&primary, first).is_ok());
    assert!(gate.wait_until_entered());
    let (second, second_send) = ordered_job(&pool, 2, &order);
    let (third, third_send) = ordered_job(&pool, 3, &order);
    assert!(route.notify(&primary, second).is_ok());
    assert!(route.notify(&primary, third).is_ok());
    assert_eq!(route.retained_len(), 1);

    let primary_join = primary
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));
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
    let pool = PendingNotificationPermitPool::new_for_test(3);
    let mut primary = CompletionRegistry::<ProducerCompletion>::new(1, 1)
        .unwrap_or_else(|error| panic!("primary notifier should start: {error}"));
    let mut route =
        PendingNotificationRoute::start(3).unwrap_or_else(|error| panic!("route start: {error}"));
    let gate = GateWake::new();
    let (first, first_send) = pending_job(&pool, &Waker::from(gate.clone()));
    assert!(route.notify(&primary, first).is_ok());
    assert!(gate.wait_until_entered());
    let order = Arc::new(Mutex::new(Vec::new()));
    let (second, second_send) = ordered_job(&pool, 2, &order);
    let (third, third_send) = ordered_job(&pool, 3, &order);
    assert!(route.notify(&primary, second).is_ok());
    assert!(route.notify(&primary, third).is_ok());

    let refusal = route
        .begin_empty_recovery_without_primary()
        .err()
        .unwrap_or_else(|| panic!("retained work must require primary ownership"));
    assert_eq!(refusal.retained_jobs, 1);

    let primary_join = primary
        .stop_notifier()
        .unwrap_or_else(|error| panic!("primary notifier should stop: {error}"));
    gate.release();
    let shutdown = route.begin_shutdown(primary_join);
    assert_eq!(
        shutdown.finish_notification_shutdown(),
        super::PendingNotificationShutdownFailures::default()
    );
    assert_eq!(*lock(&order), [2, 3]);
    assert_eq!(pool.in_use(), 0);
    drop((first_send, second_send, third_send));
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
            "recovery worker should preserve pending FIFO"
        );
        std::thread::yield_now();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
