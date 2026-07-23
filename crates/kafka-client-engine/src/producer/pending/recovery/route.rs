//! Stateful FIFO routing from the primary notifier to its prestarted recovery worker.

use crate::completion::{CompletionRegistry, CompletionRegistryError, NotifierJoin};

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

enum PendingNotificationRouteState {
    Primary(PendingNotificationBacklog),
    Recovery {
        retained: Option<PendingNotificationRecovery>,
    },
}

/// One mutable owner of primary retry order and irreversible recovery mode.
pub(crate) struct PendingNotificationRoute {
    state: PendingNotificationRouteState,
    worker: Option<PendingRecoveryWorker>,
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
        })
    }

    /// Retries every older primary job before considering the newer job.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "live pending admission installs this route in the next integration slice"
        )
    )]
    pub(crate) fn notify<T: Send + 'static>(
        &mut self,
        primary: &CompletionRegistry<T>,
        job: PendingNotificationJob,
    ) -> Result<PendingNotificationRouteMode, PendingNotificationRouteFailure> {
        if self.worker.is_none() {
            return Err(PendingNotificationRouteFailure { job });
        }
        match &mut self.state {
            PendingNotificationRouteState::Primary(backlog) => {
                match retry_primary(backlog, primary) {
                    PrimaryRetry::Ready => match primary.notify_pending(job) {
                        Ok(()) => Ok(PendingNotificationRouteMode::Primary),
                        Err((CompletionRegistryError::NotificationBackpressure, returned)) => {
                            retain_newer(backlog, returned)
                        }
                        Err((_stopped, returned)) => {
                            let recovery = take_backlog(backlog).into_recovery(returned);
                            self.state = PendingNotificationRouteState::Recovery {
                                retained: Some(recovery),
                            };
                            Ok(PendingNotificationRouteMode::Recovery)
                        }
                    },
                    PrimaryRetry::Backpressured => retain_newer(backlog, job),
                    PrimaryRetry::Stopped(returned) => {
                        backlog.push_front(returned);
                        let recovery = take_backlog(backlog).into_recovery(job);
                        self.state = PendingNotificationRouteState::Recovery {
                            retained: Some(recovery),
                        };
                        Ok(PendingNotificationRouteMode::Recovery)
                    }
                }
            }
            PendingNotificationRouteState::Recovery { retained } => {
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
}

enum PrimaryRetry {
    Ready,
    Backpressured,
    Stopped(PendingNotificationJob),
}

fn retry_primary<T: Send + 'static>(
    backlog: &mut PendingNotificationBacklog,
    primary: &CompletionRegistry<T>,
) -> PrimaryRetry {
    while let Some(job) = backlog.pop_front() {
        match primary.notify_pending(job) {
            Ok(()) => {}
            Err((CompletionRegistryError::NotificationBackpressure, returned)) => {
                backlog.push_front(returned);
                return PrimaryRetry::Backpressured;
            }
            Err((_stopped, returned)) => return PrimaryRetry::Stopped(returned),
        }
    }
    PrimaryRetry::Ready
}

fn retain_newer(
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

fn take_backlog(backlog: &mut PendingNotificationBacklog) -> PendingNotificationBacklog {
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
