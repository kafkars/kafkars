//! Constant-time FIFO retention and irreversible recovery-route ownership.

use crate::completion::NotifierJoin;

use super::{
    PendingNotificationBacklog, PendingNotificationJob, PendingNotificationRecovery,
    PendingRecoveryWorker,
    shutdown::{
        PendingNotificationShutdownOwner, PendingPrimaryMissingError, PendingRecoveryStartupOwner,
        PendingRecoveryWithoutPrimaryOwner,
    },
};

/// Current off-reactor destination for pending-send notification jobs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingNotificationRouteMode {
    Primary,
    Recovery,
}

pub(super) enum PendingNotificationRouteState {
    Primary(PendingNotificationBacklog),
    Recovery {
        retained: Option<PendingNotificationRecovery>,
    },
}

/// One mutable owner of primary retry order and irreversible recovery mode.
pub(crate) struct PendingNotificationRoute {
    pub(super) state: PendingNotificationRouteState,
    worker: Option<PendingRecoveryWorker>,
    capacity: usize,
}

impl std::fmt::Debug for PendingNotificationRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingNotificationRoute")
            .field("retained", &self.retained_len())
            .field(
                "worker_running",
                &self
                    .worker
                    .as_ref()
                    .and_then(PendingRecoveryWorker::thread_id)
                    .is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl PendingNotificationRoute {
    pub(crate) fn start(capacity: usize) -> std::io::Result<Self> {
        Ok(Self {
            state: PendingNotificationRouteState::Primary(PendingNotificationBacklog::new(
                capacity,
            )),
            worker: Some(PendingRecoveryWorker::start_prestarted(capacity)?),
            capacity,
        })
    }

    /// Retains one newer job without invoking either notification worker.
    pub(crate) fn retain_pending_notification(
        &mut self,
        job: PendingNotificationJob,
    ) -> Result<PendingNotificationRouteMode, PendingNotificationRouteFailure> {
        if self.worker.is_none() {
            return Err(PendingNotificationRouteFailure { job });
        }
        match &mut self.state {
            PendingNotificationRouteState::Primary(backlog) => retain_primary(backlog, job),
            PendingNotificationRouteState::Recovery { retained } => {
                let retained_len = retained
                    .as_ref()
                    .map_or(0, PendingNotificationRecovery::len);
                if retained_len >= self.capacity {
                    return Err(PendingNotificationRouteFailure { job });
                }
                match retained {
                    Some(older) => older.push_back(job),
                    None => *retained = Some(PendingNotificationRecovery::from_job(job)),
                }
                Ok(PendingNotificationRouteMode::Recovery)
            }
        }
    }

    /// Atomically couples the primary join to its newer terminal FIFO.
    pub(crate) fn begin_shutdown(
        &mut self,
        notifier: NotifierJoin,
    ) -> PendingNotificationShutdownOwner {
        let terminal = self.take_terminal();
        PendingNotificationShutdownOwner::from_primary(notifier, self.worker.take(), terminal)
    }

    /// Stops recovery without a primary only when no notification is retained.
    pub(crate) fn begin_empty_recovery_without_primary(
        &mut self,
    ) -> Result<PendingRecoveryWithoutPrimaryOwner, PendingPrimaryMissingError> {
        let retained_jobs = self.retained_len();
        if retained_jobs != 0 {
            return Err(PendingPrimaryMissingError { retained_jobs });
        }
        let _empty_terminal = self.take_terminal();
        Ok(PendingRecoveryWithoutPrimaryOwner::from_empty_proof(
            self.worker.take(),
        ))
    }

    /// Transfers rollback ownership when no primary notifier ever started.
    pub(crate) fn begin_startup_rollback(&mut self) -> Option<PendingRecoveryStartupOwner> {
        let terminal = self.take_terminal();
        self.worker
            .take()
            .map(|worker| PendingRecoveryStartupOwner::new(worker, terminal))
    }

    fn take_terminal(&mut self) -> Option<PendingNotificationRecovery> {
        let terminal = match &mut self.state {
            PendingNotificationRouteState::Primary(backlog) => {
                take_backlog(backlog).into_recovery_all()
            }
            PendingNotificationRouteState::Recovery { retained } => retained.take(),
        };
        self.state = PendingNotificationRouteState::Recovery { retained: None };
        terminal
    }

    pub(crate) fn worker_thread_id(&self) -> Option<std::thread::ThreadId> {
        self.worker
            .as_ref()
            .and_then(PendingRecoveryWorker::thread_id)
    }

    pub(crate) fn retained_len(&self) -> usize {
        match &self.state {
            PendingNotificationRouteState::Primary(backlog) => backlog.len(),
            PendingNotificationRouteState::Recovery { retained } => retained
                .as_ref()
                .map_or(0, PendingNotificationRecovery::len),
        }
    }

    pub(super) const fn mode(&self) -> PendingNotificationRouteMode {
        match &self.state {
            PendingNotificationRouteState::Primary(_) => PendingNotificationRouteMode::Primary,
            PendingNotificationRouteState::Recovery { .. } => {
                PendingNotificationRouteMode::Recovery
            }
        }
    }
}

fn retain_primary(
    backlog: &mut PendingNotificationBacklog,
    job: PendingNotificationJob,
) -> Result<PendingNotificationRouteMode, PendingNotificationRouteFailure> {
    backlog
        .try_push(job)
        .map(|()| PendingNotificationRouteMode::Primary)
        .map_err(|error| PendingNotificationRouteFailure {
            job: error.into_job(),
        })
}

pub(super) fn take_backlog(backlog: &mut PendingNotificationBacklog) -> PendingNotificationBacklog {
    std::mem::replace(backlog, PendingNotificationBacklog::new(0))
}

/// Impossible capacity disagreement retaining the exact newer job.
#[must_use = "the rejected pending notification job remains owned"]
pub(crate) struct PendingNotificationRouteFailure {
    job: PendingNotificationJob,
}

impl PendingNotificationRouteFailure {
    pub(crate) fn into_job(self) -> PendingNotificationJob {
        self.job
    }
}
