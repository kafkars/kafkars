//! Shared Future, blocking, lifecycle-error, and abandonment observer scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    task::{Context, Poll, Wake, Waker},
    thread::ThreadId,
    time::Duration,
};

use crate::{
    ProducerDeliveryError, ProducerDeliveryObserver, ProducerDeliveryResult, ProducerObserverError,
    completion::{CompletionRegistry, ReclaimStatus},
    delivery_test::delivered_completion,
    producer::ProducerTerminal,
};

#[test]
fn wait_and_future_observe_the_same_terminal_cell() {
    let publishing_thread = std::thread::current().id();
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("completion notifier should start")
    };
    let Ok((completion_id, inner)) = registry.reserve() else {
        panic!("completion slot should reserve")
    };
    let mut observer = ProducerDeliveryObserver::from_completion(inner);
    let wake = WakeSignal::new();

    assert_eq!(poll_once(&mut observer, Arc::clone(&wake)), Poll::Pending);
    assert_eq!(
        registry.publish(
            completion_id,
            ProducerTerminal::record(delivered_completion())
        ),
        Ok(())
    );
    let Some(waking_thread) = wake.wait() else {
        panic!("completion notifier should wake the future")
    };
    assert_ne!(waking_thread, publishing_thread);
    let metadata = match observer.wait() {
        Ok(metadata) => metadata,
        Err(error) => panic!("blocking wait should observe the same cell: {error}"),
    };
    assert_eq!(metadata.offset(), 42);
    reclaim_and_stop(&mut registry, completion_id);
}

#[test]
fn observer_lifecycle_failures_translate_explicitly() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("completion notifier should start")
    };
    let Ok((completion_id, inner)) = registry.reserve() else {
        panic!("completion slot should reserve")
    };
    let mut observer = ProducerDeliveryObserver::from_completion(inner);
    assert_eq!(
        registry.publish(
            completion_id,
            ProducerTerminal::record(delivered_completion())
        ),
        Ok(())
    );
    let wake = WakeSignal::new();
    let first = poll_until_ready(&mut observer, Arc::clone(&wake));
    assert!(first.is_ok());
    assert_eq!(
        poll_once(&mut observer, wake),
        Poll::Ready(Err(ProducerDeliveryError::Observer(
            ProducerObserverError::AlreadyObserved
        )))
    );
    reclaim_and_stop(&mut registry, completion_id);

    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("completion notifier should restart")
    };
    let Ok((stale_id, inner)) = registry.reserve() else {
        panic!("stale completion should reserve")
    };
    assert_eq!(registry.rollback_reservation(stale_id), Ok(()));
    let mut stale = ProducerDeliveryObserver::from_completion(inner);
    assert_eq!(
        poll_once(&mut stale, WakeSignal::new()),
        Poll::Ready(Err(ProducerDeliveryError::Observer(
            ProducerObserverError::Stale
        )))
    );
    stop(&mut registry);
}

#[test]
fn dropping_delivery_observer_preserves_abandon_reclaim() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("completion notifier should start")
    };
    let Ok((completion_id, inner)) = registry.reserve() else {
        panic!("completion slot should reserve")
    };
    drop(ProducerDeliveryObserver::from_completion(inner));
    assert_eq!(
        registry.publish(
            completion_id,
            ProducerTerminal::record(delivered_completion())
        ),
        Ok(())
    );
    let Ok(join) = registry.stop_notifier() else {
        panic!("settled notifier should stop")
    };
    assert_eq!(join.join_off_notifier(), Ok(()));
    assert_eq!(registry.next_reclaim(), Ok(Some(completion_id)));
    assert_eq!(
        registry.finish_reclaim(completion_id),
        Ok(ReclaimStatus::Reclaimed)
    );
}

fn poll_until_ready(
    observer: &mut ProducerDeliveryObserver,
    wake: Arc<WakeSignal>,
) -> ProducerDeliveryResult {
    match poll_once(observer, Arc::clone(&wake)) {
        Poll::Ready(result) => result,
        Poll::Pending => {
            assert!(wake.wait().is_some(), "notifier should wake the observer");
            match poll_once(observer, wake) {
                Poll::Ready(result) => result,
                Poll::Pending => panic!("woken observer should be terminal"),
            }
        }
    }
}

fn poll_once(
    observer: &mut ProducerDeliveryObserver,
    wake: Arc<WakeSignal>,
) -> Poll<ProducerDeliveryResult> {
    let waker = Waker::from(wake);
    let mut context = Context::from_waker(&waker);
    Pin::new(observer).poll(&mut context)
}

fn reclaim_and_stop(
    registry: &mut CompletionRegistry<ProducerTerminal>,
    completion_id: crate::completion::CompletionId,
) {
    assert_eq!(registry.next_reclaim(), Ok(Some(completion_id)));
    assert_eq!(
        registry.finish_reclaim(completion_id),
        Ok(ReclaimStatus::Reclaimed)
    );
    stop(registry);
}

fn stop(registry: &mut CompletionRegistry<ProducerTerminal>) {
    let Ok(join) = registry.stop_notifier() else {
        panic!("settled notifier should stop")
    };
    assert_eq!(join.join_off_notifier(), Ok(()));
}

struct WakeSignal {
    thread: Mutex<Option<ThreadId>>,
    changed: Condvar,
}

impl WakeSignal {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            thread: Mutex::new(None),
            changed: Condvar::new(),
        })
    }

    fn wait(&self) -> Option<ThreadId> {
        let result =
            self.changed
                .wait_timeout_while(lock(&self.thread), Duration::from_secs(2), |thread| {
                    thread.is_none()
                });
        let guard = match result {
            Ok((guard, _timeout)) => guard,
            Err(poison) => poison.into_inner().0,
        };
        *guard
    }
}

impl Wake for WakeSignal {
    fn wake(self: Arc<Self>) {
        *lock(&self.thread) = Some(std::thread::current().id());
        self.changed.notify_all();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
