//! Synchronization probes for pending-send observation scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    task::{Context, Poll, Wake, Waker},
    thread::ThreadId,
    time::Duration,
};

use crate::{
    completion::{CompletionObserver, CompletionObserverError},
    producer::boundary::{ProducerSend, ProducerSendResult},
};

pub(crate) struct CountingWake {
    state: Mutex<WakeState>,
    changed: Condvar,
}

struct WakeState {
    count: usize,
    thread: Option<ThreadId>,
}

impl CountingWake {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(WakeState {
                count: 0,
                thread: None,
            }),
            changed: Condvar::new(),
        })
    }

    pub(crate) fn count(&self) -> usize {
        lock(&self.state).count
    }

    pub(crate) fn wait_for_wake(&self) -> Option<ThreadId> {
        let result =
            self.changed
                .wait_timeout_while(lock(&self.state), Duration::from_secs(2), |state| {
                    state.thread.is_none()
                });
        match result {
            Ok((state, _timeout)) => state.thread,
            Err(poison) => poison.into_inner().0.thread,
        }
    }

    fn record(&self) {
        let mut state = lock(&self.state);
        state.count += 1;
        state.thread = Some(std::thread::current().id());
        self.changed.notify_all();
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

pub(crate) struct GateWake {
    state: Mutex<GateState>,
    changed: Condvar,
}

struct GateState {
    entered: bool,
    released: bool,
}

impl GateWake {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState {
                entered: false,
                released: false,
            }),
            changed: Condvar::new(),
        })
    }

    pub(crate) fn wait_until_entered(&self) -> bool {
        let result =
            self.changed
                .wait_timeout_while(lock(&self.state), Duration::from_secs(2), |state| {
                    !state.entered
                });
        match result {
            Ok((state, _timeout)) => state.entered,
            Err(poison) => poison.into_inner().0.entered,
        }
    }

    pub(crate) fn release(&self) {
        lock(&self.state).released = true;
        self.changed.notify_all();
    }
}

impl Wake for GateWake {
    fn wake(self: Arc<Self>) {
        let mut state = lock(&self.state);
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = match self.changed.wait(state) {
                Ok(state) => state,
                Err(poison) => poison.into_inner(),
            };
        }
    }
}

pub(crate) fn poll_send(
    send: &mut ProducerSend,
    wake: Arc<CountingWake>,
) -> Poll<ProducerSendResult> {
    let waker = Waker::from(wake);
    let mut context = Context::from_waker(&waker);
    Pin::new(send).poll(&mut context)
}

pub(crate) fn poll_completion<T>(
    observer: &mut CompletionObserver<T>,
    wake: Arc<GateWake>,
) -> Poll<Result<T, CompletionObserverError>> {
    let waker = Waker::from(wake);
    let mut context = Context::from_waker(&waker);
    Pin::new(observer).poll(&mut context)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
