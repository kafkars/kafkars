//! Inseparable primary-drain and recovery-worker terminal ownership.

use crate::completion::{NotifierJoin, NotifierJoinError};

use super::{PendingNotificationRecovery, PendingRecoveryJoinError, PendingRecoveryWorker};

/// Ordered failures after owned notification workers have terminated.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PendingNotificationShutdownFailures {
    pub(crate) notifier: Option<NotifierJoinError>,
    pub(crate) recovery: Option<PendingRecoveryJoinError>,
}

/// Actual owner of primary drain, recovery FIFO, and the prestarted worker.
#[must_use = "notification shutdown ownership must be finished or dropped"]
pub(crate) struct PendingNotificationShutdownOwner {
    notifier: Option<NotifierJoin>,
    worker: Option<PendingRecoveryWorker>,
    terminal: Option<PendingNotificationRecovery>,
}

impl PendingNotificationShutdownOwner {
    pub(super) const fn from_primary(
        notifier: NotifierJoin,
        worker: Option<PendingRecoveryWorker>,
        terminal: Option<PendingNotificationRecovery>,
    ) -> Self {
        Self {
            notifier: Some(notifier),
            worker,
            terminal,
        }
    }

    /// Inherently joins primary before making the terminal tail runnable.
    pub(crate) fn finish_notification_shutdown(mut self) -> PendingNotificationShutdownFailures {
        self.drain()
    }

    #[cfg(test)]
    pub(crate) fn from_handles_for_test(
        notifier: NotifierJoin,
        recovery: Option<std::thread::JoinHandle<()>>,
    ) -> Self {
        Self {
            notifier: Some(notifier),
            worker: recovery.map(PendingRecoveryWorker::from_handle_for_test),
            terminal: None,
        }
    }

    fn drain(&mut self) -> PendingNotificationShutdownFailures {
        let notifier = self
            .notifier
            .take()
            .and_then(|join| join.join_off_notifier().err());
        let recovery = drain_recovery(&mut self.worker, &mut self.terminal);
        PendingNotificationShutdownFailures { notifier, recovery }
    }
}

impl Drop for PendingNotificationShutdownOwner {
    fn drop(&mut self) {
        let _failures = self.drain();
    }
}

/// One cleanup owner used by normal and proven-empty recovery shutdown.
pub(crate) enum PendingNotificationCleanupOwner {
    Paired(PendingNotificationShutdownOwner),
    RecoveryOnly(PendingRecoveryWithoutPrimaryOwner),
}

impl PendingNotificationCleanupOwner {
    pub(crate) fn finish_notification_cleanup(self) -> PendingNotificationShutdownFailures {
        match self {
            Self::Paired(owner) => owner.finish_notification_shutdown(),
            Self::RecoveryOnly(owner) => PendingNotificationShutdownFailures {
                notifier: None,
                recovery: owner.finish().err(),
            },
        }
    }
}

/// Recovery-only owner proven to have no pending notification work.
#[must_use = "empty recovery ownership must be stopped and joined"]
pub(crate) struct PendingRecoveryWithoutPrimaryOwner {
    worker: Option<PendingRecoveryWorker>,
}

impl PendingRecoveryWithoutPrimaryOwner {
    pub(super) const fn from_empty_proof(worker: Option<PendingRecoveryWorker>) -> Self {
        Self { worker }
    }

    fn finish(mut self) -> Result<(), PendingRecoveryJoinError> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker.stop_with_terminal(None).join_off_worker()
    }
}

impl Drop for PendingRecoveryWithoutPrimaryOwner {
    fn drop(&mut self) {
        let _ = self
            .worker
            .take()
            .map(|worker| worker.stop_with_terminal(None).join_off_worker());
    }
}

/// Distinct rollback owner used only when primary startup failed.
#[must_use = "startup rollback must stop and join its prestarted worker"]
pub(crate) struct PendingRecoveryStartupOwner {
    worker: Option<PendingRecoveryWorker>,
    terminal: Option<PendingNotificationRecovery>,
}

impl PendingRecoveryStartupOwner {
    pub(super) const fn new(
        worker: PendingRecoveryWorker,
        terminal: Option<PendingNotificationRecovery>,
    ) -> Self {
        Self {
            worker: Some(worker),
            terminal,
        }
    }

    pub(crate) fn finish_startup_rollback(mut self) -> Result<(), PendingRecoveryJoinError> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker
            .stop_with_terminal(self.terminal.take())
            .join_off_worker()
    }
}

impl Drop for PendingRecoveryStartupOwner {
    fn drop(&mut self) {
        let _ = drain_recovery(&mut self.worker, &mut self.terminal);
    }
}

/// Missing primary ownership cannot authorize retained recovery dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingPrimaryMissingError {
    pub(crate) retained_jobs: usize,
}

impl std::fmt::Display for PendingPrimaryMissingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "primary notifier ownership is missing with {} retained recovery jobs",
            self.retained_jobs
        )
    }
}

impl std::error::Error for PendingPrimaryMissingError {}

fn drain_recovery(
    worker: &mut Option<PendingRecoveryWorker>,
    terminal: &mut Option<PendingNotificationRecovery>,
) -> Option<PendingRecoveryJoinError> {
    worker.take().and_then(|worker| {
        worker
            .stop_with_terminal(terminal.take())
            .join_off_worker()
            .err()
    })
}
