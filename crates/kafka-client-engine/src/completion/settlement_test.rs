//! Reserved-slot settlement, retry, abandonment, and notifier-isolation scenarios.

use std::{
    sync::{Arc, Condvar, Mutex, MutexGuard},
    task::Poll,
    thread::{self, ThreadId},
    time::Duration,
};

use super::{
    CompletionRegistry, CompletionRegistryError, ReclaimStatus, SettlementFailure,
    SettlementProgress,
    test_support::{CountingWake, finish_reclaims, poll_once, stop},
};

#[test]
fn one_pass_settles_multiple_live_observers() {
    let Ok(mut registry) = CompletionRegistry::new(3, 3) else {
        panic!("notifier should start")
    };
    let Ok((_first_id, first)) = registry.reserve() else {
        panic!("first slot should reserve")
    };
    let Ok((_second_id, second)) = registry.reserve() else {
        panic!("second slot should reserve")
    };
    let Ok((_third_id, third)) = registry.reserve() else {
        panic!("third slot should reserve")
    };
    let mut next = 10;

    let progress: SettlementProgress = registry
        .settle_reserved_with(3, |_id| {
            next += 1;
            next
        })
        .unwrap_or_else(|failure| panic!("settlement should queue: {:?}", failure.error()));

    assert_eq!(progress.queued(), 3);
    assert_eq!(progress.remaining(), 0);
    assert_eq!(first.wait(), Ok(11));
    assert_eq!(second.wait(), Ok(12));
    assert_eq!(third.wait(), Ok(13));
    assert_eq!(finish_reclaims(&mut registry), Ok(3));
    stop(&mut registry);
}

#[test]
fn published_and_reclaim_ready_values_are_never_replaced() {
    let Ok(mut registry) = CompletionRegistry::new(3, 3) else {
        panic!("notifier should start")
    };
    let Ok((published_id, published)) = registry.reserve() else {
        panic!("published slot should reserve")
    };
    let Ok((reclaim_id, reclaiming)) = registry.reserve() else {
        panic!("reclaiming slot should reserve")
    };
    let Ok((_reserved_id, reserved)) = registry.reserve() else {
        panic!("reserved slot should reserve")
    };
    assert_eq!(registry.publish(published_id, 21), Ok(()));
    assert_eq!(registry.publish(reclaim_id, 22), Ok(()));
    assert_eq!(reclaiming.wait(), Ok(22));
    assert_eq!(registry.next_reclaim(), Ok(Some(reclaim_id)));
    let mut created = 0;

    let progress = registry
        .settle_reserved_with(3, |_id| {
            created += 1;
            23
        })
        .unwrap_or_else(|failure| panic!("reserved terminal should queue: {:?}", failure.error()));

    assert_eq!(created, 1);
    assert_eq!(progress.queued(), 1);
    assert_eq!(progress.remaining(), 0);
    assert_eq!(published.wait(), Ok(21));
    assert_eq!(reserved.wait(), Ok(23));
    assert_eq!(
        registry.finish_reclaim(reclaim_id),
        Ok(ReclaimStatus::Reclaimed)
    );
    assert_eq!(finish_reclaims(&mut registry), Ok(2));
    stop(&mut registry);
}

#[test]
fn abandoned_settlement_drops_and_reclaims_on_the_notifier() {
    let caller = thread::current().id();
    let dropped = DropRecord::new();
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start")
    };
    let Ok((_id, observer)) = registry.reserve() else {
        panic!("slot should reserve")
    };
    drop(observer);
    let mut terminal = Some(DropProbe(Arc::clone(&dropped)));

    let progress = registry
        .settle_reserved_with(1, |_id| {
            terminal
                .take()
                .unwrap_or_else(|| panic!("one reserved slot needs one terminal"))
        })
        .unwrap_or_else(|failure| panic!("abandoned terminal should queue: {:?}", failure.error()));

    assert_eq!(progress.queued(), 1);
    let Some(drop_thread) = dropped.wait() else {
        panic!("notifier should drop the abandoned terminal")
    };
    assert_ne!(drop_thread, caller);
    let Ok(join) = registry.stop_notifier() else {
        panic!("settled notifier should stop")
    };
    assert_eq!(join.join(), Ok(()));
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
}

#[test]
fn bounded_retries_skip_every_already_queued_terminal() {
    let Ok(mut registry) = CompletionRegistry::new(3, 3) else {
        panic!("notifier should start")
    };
    let Ok((_first_id, first)) = registry.reserve() else {
        panic!("first slot should reserve")
    };
    let Ok((_second_id, second)) = registry.reserve() else {
        panic!("second slot should reserve")
    };
    let Ok((_third_id, third)) = registry.reserve() else {
        panic!("third slot should reserve")
    };
    let mut created = 0;

    for remaining in [2, 1, 0] {
        let progress = registry
            .settle_reserved_with(1, |_id| {
                created += 1;
                created
            })
            .unwrap_or_else(|failure| {
                panic!("bounded settlement should queue: {:?}", failure.error())
            });
        assert_eq!(progress.queued(), 1);
        assert_eq!(progress.remaining(), remaining);
    }
    let idempotent = registry
        .settle_reserved_with(3, |_id| {
            created += 1;
            created
        })
        .unwrap_or_else(|failure| panic!("empty retry should succeed: {:?}", failure.error()));

    assert_eq!(idempotent.queued(), 0);
    assert_eq!(idempotent.remaining(), 0);
    assert_eq!(created, 3);
    assert_eq!(first.wait(), Ok(1));
    assert_eq!(second.wait(), Ok(2));
    assert_eq!(third.wait(), Ok(3));
    assert_eq!(finish_reclaims(&mut registry), Ok(3));
    stop(&mut registry);
}

#[test]
fn partial_notifier_failure_retains_exact_progress_identity_and_terminal() {
    let Ok(mut registry) = CompletionRegistry::new(2, 2) else {
        panic!("notifier should start")
    };
    let Ok((_first_id, first)) = registry.reserve() else {
        panic!("first slot should reserve")
    };
    let Ok((second_id, second)) = registry.reserve() else {
        panic!("second slot should reserve")
    };
    let first_identity = Arc::new(());
    let first_progress = registry
        .settle_reserved_with(1, |_id| ExactTerminal {
            value: 41,
            identity: Arc::clone(&first_identity),
        })
        .unwrap_or_else(|failure| panic!("first terminal should queue: {:?}", failure.error()));
    assert_eq!(first_progress.queued(), 1);
    assert_eq!(first_progress.remaining(), 1);
    let Ok(join) = registry.disconnect_notifier_for_settlement_test() else {
        panic!("test notifier should stop without discarding queued work")
    };
    assert_eq!(join.join(), Ok(()));
    let failed_identity = Arc::new(());
    let failure: SettlementFailure<ExactTerminal> =
        match registry.settle_reserved_with(1, |_id| ExactTerminal {
            value: 43,
            identity: Arc::clone(&failed_identity),
        }) {
            Err(failure) => failure,
            Ok(_progress) => panic!("disconnected notifier should reject the terminal"),
        };

    assert_eq!(failure.progress().queued(), 0);
    assert_eq!(failure.progress().remaining(), 1);
    assert_eq!(failure.completion_id(), second_id);
    assert_eq!(failure.error(), CompletionRegistryError::NotifierStopped);
    let returned = failure.into_terminal();
    assert_eq!(returned.value, 43);
    assert!(Arc::ptr_eq(&returned.identity, &failed_identity));
    assert_eq!(registry.rollback_reservation(second_id), Ok(()));
    drop(second);
    let Ok(observed) = first.wait() else {
        panic!("previously queued terminal should remain observable")
    };
    assert_eq!(observed.value, 41);
    assert!(Arc::ptr_eq(&observed.identity, &first_identity));
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
}

#[test]
fn hostile_waker_runs_only_on_the_completion_notifier() {
    let caller = thread::current().id();
    let wake = CountingWake::new();
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start")
    };
    let Ok((_id, mut observer)) = registry.reserve() else {
        panic!("slot should reserve")
    };
    assert_eq!(poll_once(&mut observer, Arc::clone(&wake)), Poll::Pending);

    let progress = registry
        .settle_reserved_with(1, |_id| 31)
        .unwrap_or_else(|failure| panic!("terminal should queue: {:?}", failure.error()));

    assert_eq!(progress.queued(), 1);
    let Some(wake_thread) = wake.wait_for_wake() else {
        panic!("notifier should run the hostile waker")
    };
    assert_ne!(wake_thread, caller);
    assert_eq!(poll_once(&mut observer, wake), Poll::Ready(Ok(31)));
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
    stop(&mut registry);
}

struct ExactTerminal {
    value: u8,
    identity: Arc<()>,
}

struct DropProbe(Arc<DropRecord>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.record();
    }
}

struct DropRecord {
    thread: Mutex<Option<ThreadId>>,
    changed: Condvar,
}

impl DropRecord {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            thread: Mutex::new(None),
            changed: Condvar::new(),
        })
    }

    fn record(&self) {
        *lock(&self.thread) = Some(thread::current().id());
        self.changed.notify_all();
    }

    fn wait(&self) -> Option<ThreadId> {
        wait_for_thread(&self.thread, &self.changed)
    }
}

fn wait_for_thread(state: &Mutex<Option<ThreadId>>, changed: &Condvar) -> Option<ThreadId> {
    let result =
        changed.wait_timeout_while(lock(state), Duration::from_secs(2), |value| value.is_none());
    let guard = match result {
        Ok((guard, _timeout)) => guard,
        Err(poison) => poison.into_inner().0,
    };
    *guard
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
