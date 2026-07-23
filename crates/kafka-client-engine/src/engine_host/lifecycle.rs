//! Retained shutdown report shared without holding locks across native cleanup.

use std::{
    sync::{Condvar, Mutex, MutexGuard},
    thread::{self, ThreadId},
};

use super::{EngineHostControl, EngineHostError, EngineShutdownError};

pub(crate) struct EngineLifecycle {
    state: Mutex<LifecycleState>,
    changed: Condvar,
    notifier_thread: Mutex<Option<ThreadId>>,
}

enum LifecycleState {
    Running,
    Closing,
    Closed(Option<EngineShutdownError>),
}

impl EngineLifecycle {
    pub(crate) const fn new() -> Self {
        Self {
            state: Mutex::new(LifecycleState::Running),
            changed: Condvar::new(),
            notifier_thread: Mutex::new(None),
        }
    }

    pub(crate) fn install_notifier_thread(&self, thread_id: ThreadId) {
        *lock(&self.notifier_thread) = Some(thread_id);
    }

    pub(crate) fn request(&self, control: &EngineHostControl) {
        let mut state = lock(&self.state);
        if matches!(*state, LifecycleState::Running) {
            *state = LifecycleState::Closing;
        }
        drop(state);
        control.request_shutdown();
    }

    pub(crate) fn request_and_wait(
        &self,
        control: &EngineHostControl,
    ) -> Result<(), EngineShutdownError> {
        self.request(control);
        if self.is_notifier_thread() {
            return Err(EngineShutdownError::notifier_thread());
        }
        let mut state = lock(&self.state);
        loop {
            match &*state {
                LifecycleState::Closed(None) => return Ok(()),
                LifecycleState::Closed(Some(error)) => return Err(error.clone()),
                LifecycleState::Running | LifecycleState::Closing => {
                    state = wait(&self.changed, state);
                }
            }
        }
    }

    pub(crate) fn publish(&self, failure: Option<&EngineHostError>) {
        let report = failure.map(EngineShutdownError::host);
        *lock(&self.state) = LifecycleState::Closed(report);
        self.changed.notify_all();
    }

    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        matches!(*lock(&self.state), LifecycleState::Closed(_))
    }

    #[cfg(test)]
    pub(crate) fn is_closing(&self) -> bool {
        matches!(*lock(&self.state), LifecycleState::Closing)
    }

    #[cfg(test)]
    pub(crate) fn wait_closed(&self, timeout: std::time::Duration) -> bool {
        let state = lock(&self.state);
        let result = self.changed.wait_timeout_while(state, timeout, |state| {
            !matches!(*state, LifecycleState::Closed(_))
        });
        let state = match result {
            Ok((state, _timeout)) => state,
            Err(poisoned) => poisoned.into_inner().0,
        };
        matches!(*state, LifecycleState::Closed(_))
    }

    #[cfg(test)]
    pub(crate) fn closed_error(&self) -> Option<String> {
        match &*lock(&self.state) {
            LifecycleState::Closed(Some(error)) => Some(error.to_string()),
            LifecycleState::Running | LifecycleState::Closing | LifecycleState::Closed(None) => {
                None
            }
        }
    }

    fn is_notifier_thread(&self) -> bool {
        lock(&self.notifier_thread).is_some_and(|thread_id| thread_id == thread::current().id())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn wait<'a>(
    changed: &Condvar,
    state: MutexGuard<'a, LifecycleState>,
) -> MutexGuard<'a, LifecycleState> {
    changed
        .wait(state)
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
