//! Synchronization helpers for completion observer scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread::ThreadId,
    time::Duration,
};

use super::{
    CompletionId, CompletionObserver, CompletionObserverError, CompletionRegistry,
    CompletionRegistryError, ReclaimStatus,
};

/// Holds one completion cell lock until the caller releases the returned gate.
pub(crate) fn hold_cell_lock<T: Send + 'static>(
    registry: &CompletionRegistry<T>,
    id: CompletionId,
) -> Option<(std::sync::mpsc::SyncSender<()>, std::thread::JoinHandle<()>)> {
    let cell = registry.cell_for_test(id)?;
    let (entered_sender, entered) = std::sync::mpsc::sync_channel(0);
    let (release, released) = std::sync::mpsc::sync_channel(0);
    let handle = std::thread::spawn(move || {
        let _guard = cell.lock_for_test();
        let _entered = entered_sender.send(());
        let _released = released.recv();
    });
    if entered.recv().is_err() {
        return None;
    }
    Some((release, handle))
}

pub(super) struct CountingWake {
    count: AtomicUsize,
    state: Mutex<Option<ThreadId>>,
    changed: Condvar,
}

impl CountingWake {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
            state: Mutex::new(None),
            changed: Condvar::new(),
        })
    }

    pub(super) fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    pub(super) fn wait_for_wake(&self) -> Option<ThreadId> {
        let guard = lock(&self.state);
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

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.record();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.record();
    }
}

impl CountingWake {
    fn record(&self) {
        self.count.fetch_add(1, Ordering::AcqRel);
        *lock(&self.state) = Some(std::thread::current().id());
        self.changed.notify_all();
    }
}

pub(super) struct GateWake {
    state: Mutex<GateState>,
    changed: Condvar,
}

struct GateState {
    entered: bool,
    released: bool,
}

impl GateWake {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState {
                entered: false,
                released: false,
            }),
            changed: Condvar::new(),
        })
    }

    pub(super) fn wait_until_entered(&self) -> bool {
        let guard = lock(&self.state);
        let result = self
            .changed
            .wait_timeout_while(guard, Duration::from_secs(2), |state| !state.entered);
        let guard = match result {
            Ok((guard, _timeout)) => guard,
            Err(poison) => poison.into_inner().0,
        };
        guard.entered
    }

    pub(super) fn release(&self) {
        lock(&self.state).released = true;
        self.changed.notify_all();
    }

    pub(super) fn block_until_released(&self) {
        let mut state = lock(&self.state);
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = match self.changed.wait(state) {
                Ok(next) => next,
                Err(poison) => poison.into_inner(),
            };
        }
    }
}

impl Wake for GateWake {
    fn wake(self: Arc<Self>) {
        self.block_until_released();
    }
}

pub(super) struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("intentional completion-waker panic");
    }
}

pub(super) fn poll_once<T>(
    observer: &mut CompletionObserver<T>,
    wake: Arc<impl Wake + Send + Sync + 'static>,
) -> Poll<Result<T, CompletionObserverError>> {
    let waker = Waker::from(wake);
    let mut context = Context::from_waker(&waker);
    Pin::new(observer).poll(&mut context)
}

pub(super) fn finish_reclaims<T: Send + 'static>(
    registry: &mut CompletionRegistry<T>,
) -> Result<usize, CompletionRegistryError> {
    let mut count = 0;
    while let Some(id) = registry.next_reclaim()? {
        loop {
            match registry.finish_reclaim(id)? {
                ReclaimStatus::Reclaimed => {
                    count += 1;
                    break;
                }
                ReclaimStatus::Retry => std::thread::yield_now(),
            }
        }
    }
    Ok(count)
}

pub(super) fn stop<T: Send + 'static>(registry: &mut CompletionRegistry<T>) {
    let Ok(join) = registry.stop_notifier() else {
        panic!("notifier should stop");
    };
    assert_eq!(join.join_off_notifier(), Ok(()));
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
