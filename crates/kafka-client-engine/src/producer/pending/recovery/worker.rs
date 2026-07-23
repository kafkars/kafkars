//! Prestarted off-reactor worker for exact pending-notification recovery batches.

use std::{
    fmt,
    sync::Arc,
    thread::{self, JoinHandle, ThreadId},
};

use super::{
    PendingNotificationRecovery,
    queue::{PendingRecoveryQueue, PendingRecoverySubmitError},
};

const RECOVERY_THREAD_NAME: &str = "kafka-client-pending-recovery";

/// Sole producer-side owner of the dedicated recovery thread.
pub(crate) struct PendingRecoveryWorker {
    queue: Arc<PendingRecoveryQueue>,
    handle: Option<JoinHandle<()>>,
}

impl PendingRecoveryWorker {
    pub(crate) fn start_prestarted(capacity: usize) -> std::io::Result<Self> {
        let queue = Arc::new(PendingRecoveryQueue::new(capacity));
        let worker_queue = Arc::clone(&queue);
        let handle = thread::Builder::new()
            .name(RECOVERY_THREAD_NAME.to_owned())
            .spawn(move || run(&worker_queue))?;
        Ok(Self {
            queue,
            handle: Some(handle),
        })
    }

    pub(super) fn try_submit(
        &self,
        recovery: PendingNotificationRecovery,
    ) -> Result<(), PendingRecoverySubmitError> {
        self.queue.try_submit(recovery)
    }

    #[cfg(test)]
    pub(crate) fn stop(&mut self) -> Option<PendingRecoveryJoin> {
        self.queue.close();
        self.handle.take().map(|handle| PendingRecoveryJoin {
            handle: Some(handle),
        })
    }

    /// Closes with one FIFO tail that becomes runnable only after primary
    /// notifier drain ownership has been discharged.
    pub(super) fn stop_with_terminal(
        mut self,
        terminal: Option<PendingNotificationRecovery>,
    ) -> PendingRecoveryJoin {
        self.queue.close_with_terminal(terminal);
        PendingRecoveryJoin {
            handle: self.handle.take(),
        }
    }

    pub(crate) fn thread_id(&self) -> Option<ThreadId> {
        self.handle.as_ref().map(|handle| handle.thread().id())
    }
}

impl Drop for PendingRecoveryWorker {
    fn drop(&mut self) {
        self.queue.close();
        let Some(handle) = self.handle.take() else {
            return;
        };
        // The worker closure owns only an Arc queue. This linear owner never
        // enters that closure, so destructor joining cannot be a self-join.
        let _join_result = handle.join();
    }
}

fn run(queue: &PendingRecoveryQueue) {
    while let Some(recovery) = queue.next() {
        recovery.run_off_reactor();
    }
}

/// Join ownership transferred away from the producer reactor.
#[must_use = "the recovery worker must be joined by terminal host finalization"]
pub(crate) struct PendingRecoveryJoin {
    handle: Option<JoinHandle<()>>,
}

impl PendingRecoveryJoin {
    /// Joins off-worker or preserves the exact handle on a self-thread call.
    pub(crate) fn join(mut self) -> PendingRecoveryJoinOutcome {
        let Some(handle) = self.handle.as_ref() else {
            return PendingRecoveryJoinOutcome::Joined(Ok(()));
        };
        if handle.thread().id() == thread::current().id() {
            return PendingRecoveryJoinOutcome::SelfThread(self);
        }
        let Some(handle) = self.handle.take() else {
            return PendingRecoveryJoinOutcome::Joined(Ok(()));
        };
        PendingRecoveryJoinOutcome::Joined(
            handle
                .join()
                .map_err(|_panic| PendingRecoveryJoinError::Panicked),
        )
    }

    pub(crate) fn join_off_worker(mut self) -> Result<(), PendingRecoveryJoinError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        handle
            .join()
            .map_err(|_panic| PendingRecoveryJoinError::Panicked)
    }

    #[cfg(test)]
    pub(crate) const fn from_handle_for_test(handle: JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
        }
    }
}

/// Result that never discards a recovery-worker handle on its own thread.
#[must_use = "a self-thread outcome retains recovery-worker join ownership"]
pub(crate) enum PendingRecoveryJoinOutcome {
    Joined(Result<(), PendingRecoveryJoinError>),
    SelfThread(PendingRecoveryJoin),
}

/// Failure to finish recovery-thread ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingRecoveryJoinError {
    Panicked,
}

impl fmt::Display for PendingRecoveryJoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("pending recovery worker panicked")
    }
}

impl std::error::Error for PendingRecoveryJoinError {}

#[cfg(test)]
impl PendingRecoveryWorker {
    pub(super) fn from_handle_for_test(handle: JoinHandle<()>) -> Self {
        Self {
            queue: Arc::new(PendingRecoveryQueue::new(0)),
            handle: Some(handle),
        }
    }
}
