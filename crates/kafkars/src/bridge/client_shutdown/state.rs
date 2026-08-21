//! Bounded registration and terminal state for the one native shutdown worker.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle},
};

use kafka_client_engine::{Engine, EngineShutdownError};

use crate::{ErrorKind, KafkaError};

const ASYNC_OBSERVER_CAPACITY: usize = 16;
const SHUTDOWN_THREAD_NAME: &str = "kafka-client-shutdown";

pub(super) struct ShutdownShared {
    engine: Engine,
    state: Mutex<ShutdownState>,
    ready: Condvar,
}

struct ShutdownState {
    phase: ShutdownPhase,
    next_registration: u64,
    wakers: Vec<ShutdownWaker>,
    worker: Option<JoinHandle<()>>,
}

enum ShutdownPhase {
    Open,
    Running,
    Closed(Result<(), KafkaError>),
}

struct ShutdownWaker {
    registration: u64,
    waker: Waker,
}

impl ShutdownShared {
    pub(super) fn try_new(engine: Engine) -> Result<Arc<Self>, KafkaError> {
        let mut wakers = Vec::new();
        wakers
            .try_reserve_exact(ASYNC_OBSERVER_CAPACITY)
            .map_err(|_error| {
                KafkaError::new(
                    ErrorKind::Internal,
                    "failed to reserve bounded shutdown observer capacity",
                )
            })?;
        Ok(Arc::new(Self {
            engine,
            state: Mutex::new(ShutdownState {
                phase: ShutdownPhase::Open,
                next_registration: 0,
                wakers,
                worker: None,
            }),
            ready: Condvar::new(),
        }))
    }

    pub(super) fn begin(self: &Arc<Self>) {
        let mut state = self.lock();
        if !matches!(state.phase, ShutdownPhase::Open) {
            return;
        }
        self.engine.request_shutdown();
        state.phase = ShutdownPhase::Running;
        let shared = Arc::clone(self);
        match thread::Builder::new()
            .name(SHUTDOWN_THREAD_NAME.to_owned())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| shared.engine.shutdown()))
                    .map_err(|_panic| shutdown_worker_panicked())
                    .and_then(|result| result.map_err(|error| translate_shutdown(&error)));
                shared.complete(result);
            }) {
            Ok(worker) => state.worker = Some(worker),
            Err(error) => {
                state.phase = ShutdownPhase::Closed(Err(KafkaError::new(
                    ErrorKind::Internal,
                    format!("failed to start client shutdown worker: {error}"),
                )));
                self.ready.notify_all();
            }
        }
    }

    pub(super) fn poll(
        &self,
        registration: &mut Option<u64>,
        context: &Context<'_>,
    ) -> Poll<Result<(), KafkaError>> {
        let mut state = self.lock();
        if let ShutdownPhase::Closed(result) = &state.phase {
            *registration = None;
            return Poll::Ready(result.clone());
        }
        if let Some(current) = *registration {
            if let Some(stored) = state
                .wakers
                .iter_mut()
                .find(|stored| stored.registration == current)
            {
                if !stored.waker.will_wake(context.waker()) {
                    stored.waker.clone_from(context.waker());
                }
                return Poll::Pending;
            }
            *registration = None;
        }
        if state.wakers.len() == ASYNC_OBSERVER_CAPACITY {
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        let Some(next) = state.next_registration.checked_add(1) else {
            context.waker().wake_by_ref();
            return Poll::Pending;
        };
        let current = state.next_registration;
        state.next_registration = next;
        state.wakers.push(ShutdownWaker {
            registration: current,
            waker: context.waker().clone(),
        });
        *registration = Some(current);
        Poll::Pending
    }

    pub(super) fn wait(&self) -> Result<(), KafkaError> {
        let mut state = self.lock();
        loop {
            match &state.phase {
                ShutdownPhase::Closed(result) => return result.clone(),
                ShutdownPhase::Open | ShutdownPhase::Running => {
                    state = self
                        .ready
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            }
        }
    }

    pub(super) fn unregister(&self, registration: &mut Option<u64>) {
        let Some(registration) = registration.take() else {
            return;
        };
        let mut state = self.lock();
        if let Some(index) = state
            .wakers
            .iter()
            .position(|stored| stored.registration == registration)
        {
            state.wakers.swap_remove(index);
        }
    }

    fn complete(&self, result: Result<(), KafkaError>) {
        let wakers = {
            let mut state = self.lock();
            if !matches!(state.phase, ShutdownPhase::Running) {
                return;
            }
            state.phase = ShutdownPhase::Closed(result);
            self.ready.notify_all();
            std::mem::take(&mut state.wakers)
        };
        for registration in wakers {
            registration.waker.wake();
        }
    }

    pub(super) fn join_worker(&self) {
        let worker = {
            let mut state = self.lock();
            let Some(worker) = state.worker.as_ref() else {
                return;
            };
            if worker.thread().id() == thread::current().id() {
                return;
            }
            state.worker.take()
        };
        if let Some(worker) = worker {
            let _join_result = worker.join();
        }
    }

    fn lock(&self) -> MutexGuard<'_, ShutdownState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for ShutdownShared {
    fn drop(&mut self) {
        let worker = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .worker
            .take();
        if let Some(worker) = worker {
            if worker.thread().id() != thread::current().id() {
                let _join_result = worker.join();
            }
        }
    }
}

fn translate_shutdown(error: &EngineShutdownError) -> KafkaError {
    KafkaError::new(ErrorKind::Internal, error.to_string())
}

fn shutdown_worker_panicked() -> KafkaError {
    KafkaError::new(ErrorKind::Internal, "client shutdown worker panicked")
}
