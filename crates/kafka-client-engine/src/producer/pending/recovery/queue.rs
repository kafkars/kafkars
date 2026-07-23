//! Fixed-capacity transport of whole pending-notification recovery batches.

use std::{
    collections::VecDeque,
    sync::{Condvar, Mutex, MutexGuard},
};

use super::PendingNotificationRecovery;

/// Why a recovery queue returned its exact batch to the route owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingRecoverySubmitErrorKind {
    Full,
    Stopped,
}

/// Failed recovery admission retaining every notification in FIFO order.
#[must_use = "the exact recovery batch remains owned by this failure"]
pub(super) struct PendingRecoverySubmitError {
    kind: PendingRecoverySubmitErrorKind,
    recovery: PendingNotificationRecovery,
}

impl PendingRecoverySubmitError {
    pub(super) const fn kind(&self) -> PendingRecoverySubmitErrorKind {
        self.kind
    }

    pub(super) fn into_recovery(self) -> PendingNotificationRecovery {
        self.recovery
    }
}

pub(crate) struct PendingRecoveryQueue {
    capacity: usize,
    state: Mutex<PendingRecoveryQueueState>,
    changed: Condvar,
}

struct PendingRecoveryQueueState {
    open: bool,
    retained_jobs: usize,
    recoveries: VecDeque<PendingNotificationRecovery>,
    terminal: Option<PendingNotificationRecovery>,
}

impl PendingRecoveryQueue {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(PendingRecoveryQueueState {
                open: true,
                retained_jobs: 0,
                recoveries: VecDeque::with_capacity(capacity),
                terminal: None,
            }),
            changed: Condvar::new(),
        }
    }

    pub(super) fn try_submit(
        &self,
        recovery: PendingNotificationRecovery,
    ) -> Result<(), PendingRecoverySubmitError> {
        let mut state = self.lock();
        if !state.open {
            return Err(PendingRecoverySubmitError {
                kind: PendingRecoverySubmitErrorKind::Stopped,
                recovery,
            });
        }
        let Some(retained_jobs) = state.retained_jobs.checked_add(recovery.len()) else {
            return Err(PendingRecoverySubmitError {
                kind: PendingRecoverySubmitErrorKind::Full,
                recovery,
            });
        };
        if retained_jobs > self.capacity {
            return Err(PendingRecoverySubmitError {
                kind: PendingRecoverySubmitErrorKind::Full,
                recovery,
            });
        }
        state.retained_jobs = retained_jobs;
        state.recoveries.push_back(recovery);
        self.changed.notify_one();
        Ok(())
    }

    pub(super) fn next(&self) -> Option<PendingNotificationRecovery> {
        let mut state = self.lock();
        loop {
            if let Some(recovery) = state.recoveries.pop_front() {
                state.retained_jobs -= recovery.len();
                return Some(recovery);
            }
            if let Some(recovery) = state.terminal.take() {
                return Some(recovery);
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

    /// Installs the one terminal tail after the primary notifier has drained.
    ///
    /// The tail does not compete for normal queue capacity: its jobs already
    /// own permits from the same global `P` pool, and closing atomically
    /// prevents any later normal submission.
    pub(super) fn close_with_terminal(&self, terminal: Option<PendingNotificationRecovery>) {
        let mut state = self.lock();
        state.open = false;
        state.terminal = terminal;
        self.changed.notify_all();
    }

    fn lock(&self) -> MutexGuard<'_, PendingRecoveryQueueState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wait<'a>(
        &self,
        state: MutexGuard<'a, PendingRecoveryQueueState>,
    ) -> MutexGuard<'a, PendingRecoveryQueueState> {
        self.changed
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
