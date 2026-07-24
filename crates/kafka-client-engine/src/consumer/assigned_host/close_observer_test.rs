//! Runtime-neutral close observation over the shared completion cell.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    task::{Context, Poll, Wake, Waker},
    thread::ThreadId,
    time::Duration,
};

use kafka_client_core::{AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine};

use crate::completion::CompletionRegistry;

use super::{
    close_observer::{
        AssignedConsumerCloseObserver, AssignedConsumerCloseObserverError,
        AssignedConsumerCloseTerminal,
    },
    completion::AssignedConsumerCompletionNotifier,
};

#[test]
fn blocking_wait_observes_the_same_notifier_terminal() {
    let (mut notifier, publishers) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start notifier: {error}"));
    let mut completions = CompletionRegistry::with_publisher(1, publishers.close);
    let (completion_id, observer) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("reserve close: {error}"));
    let close_id = close_id();
    completions
        .publish(
            completion_id,
            AssignedConsumerCloseTerminal::Closed(close_id),
        )
        .unwrap_or_else(|(error, _terminal)| panic!("publish close: {error}"));

    assert!(
        AssignedConsumerCloseObserver::from_completion(observer)
            .wait()
            .is_ok()
    );
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}

#[test]
fn execution_unavailable_is_a_stable_terminal_error() {
    let (mut notifier, publishers) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start notifier: {error}"));
    let mut completions = CompletionRegistry::with_publisher(1, publishers.close);
    let (completion_id, observer) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("reserve close: {error}"));
    completions
        .publish(
            completion_id,
            AssignedConsumerCloseTerminal::ExecutionUnavailable,
        )
        .unwrap_or_else(|(error, _terminal)| panic!("publish close: {error}"));

    assert_eq!(
        AssignedConsumerCloseObserver::from_completion(observer).wait(),
        Err(AssignedConsumerCloseObserverError::ExecutionUnavailable)
    );
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}

fn close_id() -> kafka_client_core::AssignedConsumerCloseId {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("accept close: {error}"));
    let AssignedConsumerEffect::AcceptClose { close_id } = transition.effects()[0] else {
        panic!("first close effect must accept");
    };
    close_id
}

#[test]
fn observer_drop_abandons_without_revoking_terminal_authority() {
    let (mut notifier, publishers) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start notifier: {error}"));
    let mut completions = CompletionRegistry::with_publisher(1, publishers.close);
    let (completion_id, observer) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("reserve close: {error}"));
    drop(AssignedConsumerCloseObserver::from_completion(observer));

    completions
        .publish(
            completion_id,
            AssignedConsumerCloseTerminal::ExecutionUnavailable,
        )
        .unwrap_or_else(|(error, _terminal)| panic!("publish abandoned close: {error}"));

    assert_eq!(completions.unsettled_len(), 0);
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}

#[test]
fn close_waker_runs_only_on_the_assigned_consumer_notifier() {
    let caller = std::thread::current().id();
    let (mut notifier, publishers) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start notifier: {error}"));
    let mut completions = CompletionRegistry::with_publisher(1, publishers.close);
    let (completion_id, observer) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("reserve close: {error}"));
    let mut observer = AssignedConsumerCloseObserver::from_completion(observer);
    let wake = TrackingWake::new();

    assert_eq!(poll_once(&mut observer, Arc::clone(&wake)), Poll::Pending);
    completions
        .publish(
            completion_id,
            AssignedConsumerCloseTerminal::Closed(close_id()),
        )
        .unwrap_or_else(|(error, _terminal)| panic!("publish close: {error}"));

    let wake_thread = wake
        .wait()
        .unwrap_or_else(|| panic!("assigned-consumer notifier should wake observer"));
    assert_ne!(wake_thread, caller);
    assert_eq!(poll_once(&mut observer, wake), Poll::Ready(Ok(())));
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}

struct TrackingWake {
    thread: Mutex<Option<ThreadId>>,
    changed: Condvar,
}

impl TrackingWake {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            thread: Mutex::new(None),
            changed: Condvar::new(),
        })
    }

    fn wait(&self) -> Option<ThreadId> {
        let guard = lock(&self.thread);
        let result = self
            .changed
            .wait_timeout_while(guard, Duration::from_secs(2), |thread| thread.is_none());
        let guard = match result {
            Ok((guard, _timeout)) => guard,
            Err(poison) => poison.into_inner().0,
        };
        *guard
    }
}

impl Wake for TrackingWake {
    fn wake(self: Arc<Self>) {
        *lock(&self.thread) = Some(std::thread::current().id());
        self.changed.notify_all();
    }
}

fn poll_once(
    observer: &mut AssignedConsumerCloseObserver,
    wake: Arc<TrackingWake>,
) -> Poll<Result<(), AssignedConsumerCloseObserverError>> {
    let waker = Waker::from(wake);
    let mut context = Context::from_waker(&waker);
    Pin::new(observer).poll(&mut context)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
