//! One bounded queue shared by terminal publications and pending-send signals.

use std::{
    collections::VecDeque,
    sync::{Condvar, Mutex, MutexGuard},
};

use crate::producer::pending::PendingNotificationJob;

use super::notifier::PublishJob;

pub(super) enum NotificationJob<T> {
    Publish(PublishJob<T>),
    Pending(PendingNotificationJob),
}

pub(super) enum QueuePushError<J> {
    Full(J),
    Closed(J),
}

/// One global FIFO capacity shared by both typed notification classes.
///
/// Submission is non-blocking. Saturation and closure return the exact job;
/// neither condition authorizes synchronous dispatch by the submitting owner.
pub(super) struct NotificationQueue<T> {
    capacity: usize,
    state: Mutex<QueueState<T>>,
    changed: Condvar,
}

struct QueueState<T> {
    open: bool,
    jobs: VecDeque<NotificationJob<T>>,
}

impl<T> NotificationQueue<T> {
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

    pub(super) fn try_publish(
        &self,
        job: PublishJob<T>,
    ) -> Result<(), QueuePushError<PublishJob<T>>> {
        let mut state = self.lock();
        if !state.open {
            return Err(QueuePushError::Closed(job));
        }
        if state.jobs.len() == self.capacity {
            return Err(QueuePushError::Full(job));
        }
        state.jobs.push_back(NotificationJob::Publish(job));
        self.changed.notify_one();
        Ok(())
    }

    pub(super) fn try_pending(
        &self,
        job: PendingNotificationJob,
    ) -> Result<(), QueuePushError<PendingNotificationJob>> {
        let mut state = self.lock();
        if !state.open {
            return Err(QueuePushError::Closed(job));
        }
        if state.jobs.len() == self.capacity {
            return Err(QueuePushError::Full(job));
        }
        state.jobs.push_back(NotificationJob::Pending(job));
        self.changed.notify_one();
        Ok(())
    }

    pub(super) fn next(&self) -> Option<NotificationJob<T>> {
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

    fn lock(&self) -> MutexGuard<'_, QueueState<T>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wait<'a>(&self, state: MutexGuard<'a, QueueState<T>>) -> MutexGuard<'a, QueueState<T>> {
        self.changed
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
