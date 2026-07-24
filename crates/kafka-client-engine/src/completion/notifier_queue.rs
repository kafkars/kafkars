//! One bounded queue for accepted-operation terminal publication.

use std::{
    collections::VecDeque,
    sync::{Condvar, Mutex, MutexGuard},
};

pub(crate) enum QueuePushError<J> {
    Full(J),
    Closed(J),
}

/// One FIFO sized to the accepted-operation completion capacity.
///
/// Submission is non-blocking. Saturation and closure return the exact job;
/// neither condition authorizes synchronous dispatch by the submitting owner.
pub(super) struct NotificationQueue<J> {
    capacity: usize,
    state: Mutex<QueueState<J>>,
    changed: Condvar,
}

struct QueueState<J> {
    open: bool,
    jobs: VecDeque<J>,
}

impl<J> NotificationQueue<J> {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(QueueState {
                open: true,
                jobs: VecDeque::with_capacity(capacity),
            }),
            changed: Condvar::new(),
        }
    }

    pub(super) fn try_publish(&self, job: J) -> Result<(), QueuePushError<J>> {
        let mut state = self.lock();
        if !state.open {
            return Err(QueuePushError::Closed(job));
        }
        if state.jobs.len() == self.capacity {
            return Err(QueuePushError::Full(job));
        }
        state.jobs.push_back(job);
        self.changed.notify_one();
        Ok(())
    }

    pub(super) fn try_publish_with<I>(
        &self,
        input: I,
        wrap: fn(I) -> J,
    ) -> Result<(), QueuePushError<I>> {
        let mut state = self.lock();
        if !state.open {
            return Err(QueuePushError::Closed(input));
        }
        if state.jobs.len() == self.capacity {
            return Err(QueuePushError::Full(input));
        }
        state.jobs.push_back(wrap(input));
        self.changed.notify_one();
        Ok(())
    }

    pub(super) fn next(&self) -> Option<J> {
        let mut state = self.lock();
        loop {
            if let Some(job) = state.jobs.pop_front() {
                return Some(job);
            }
            if !state.open {
                return None;
            }
            state = self.wait(state);
        }
    }

    pub(super) fn close(&self) {
        self.lock().open = false;
        self.changed.notify_all();
    }

    fn lock(&self) -> MutexGuard<'_, QueueState<J>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wait<'a>(&self, state: MutexGuard<'a, QueueState<J>>) -> MutexGuard<'a, QueueState<J>> {
        self.changed
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
