//! Bounded FIFO retention and off-reactor recovery of pending notification jobs.

use std::collections::VecDeque;

use super::super::{PendingNotificationDispatchAuthority, PendingNotificationJob};

#[path = "backlog/authority.rs"]
mod authority;
pub(crate) use authority::PendingNotificationRecoveryDispatchOwner;

/// Host-owned fixed capacity for exact jobs rejected by a full notifier FIFO.
pub(crate) struct PendingNotificationBacklog {
    capacity: usize,
    jobs: VecDeque<PendingNotificationJob>,
}

impl PendingNotificationBacklog {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            jobs: VecDeque::with_capacity(capacity),
        }
    }

    /// Retains one exact full-queue job without dispatching application code.
    pub(crate) fn try_push(
        &mut self,
        job: PendingNotificationJob,
    ) -> Result<(), PendingNotificationBacklogFull> {
        if self.jobs.len() >= self.capacity {
            return Err(PendingNotificationBacklogFull { job });
        }
        self.jobs.push_back(job);
        Ok(())
    }

    /// Returns the oldest retained job for a primary-notifier retry.
    pub(crate) fn pop_front(&mut self) -> Option<PendingNotificationJob> {
        self.jobs.pop_front()
    }

    /// Restores a failed oldest retry without moving it behind newer work.
    pub(crate) fn push_front(&mut self, job: PendingNotificationJob) {
        self.jobs.push_front(job);
    }

    /// Transfers older retained jobs plus the exact closed-queue return.
    pub(crate) fn into_recovery(
        mut self,
        returned: PendingNotificationJob,
    ) -> PendingNotificationRecovery {
        self.jobs.push_back(returned);
        PendingNotificationRecovery { jobs: self.jobs }
    }

    pub(crate) fn len(&self) -> usize {
        self.jobs.len()
    }

    /// Transfers every primary retry when terminal shutdown ends that route.
    pub(crate) fn into_recovery_all(self) -> Option<PendingNotificationRecovery> {
        (!self.jobs.is_empty()).then_some(PendingNotificationRecovery { jobs: self.jobs })
    }
}

/// Full backlog rejection retaining the exact submitted job.
#[must_use = "the pending notification job remains owned by this failure"]
pub(crate) struct PendingNotificationBacklogFull {
    job: PendingNotificationJob,
}

impl PendingNotificationBacklogFull {
    pub(crate) fn into_job(self) -> PendingNotificationJob {
        self.job
    }
}

/// Named owner transferred to a thread that is not a host or driver reactor.
#[must_use = "off-reactor recovery owns live notification permits and wakers"]
pub(crate) struct PendingNotificationRecovery {
    jobs: VecDeque<PendingNotificationJob>,
}

impl PendingNotificationRecovery {
    /// Wraps one later job after the route has entered recovery mode.
    pub(super) fn from_job(job: PendingNotificationJob) -> Self {
        Self {
            jobs: VecDeque::from([job]),
        }
    }

    /// Appends newer work behind every job already awaiting recovery.
    pub(super) fn push_back(&mut self, job: PendingNotificationJob) {
        self.jobs.push_back(job);
    }

    pub(super) fn len(&self) -> usize {
        self.jobs.len()
    }

    /// Dispatches one exact FIFO batch on the dedicated recovery worker.
    pub(super) fn run_off_reactor(mut self) {
        let authority = PendingNotificationDispatchAuthority::from_recovery(
            PendingNotificationRecoveryDispatchOwner::new(),
        );
        while let Some(job) = self.jobs.pop_front() {
            job.dispatch_pending_notification(&authority);
        }
    }

    #[cfg(test)]
    pub(crate) fn dispatch_all_pending_notifications_for_test(self) {
        self.run_off_reactor();
    }

    #[cfg(test)]
    pub(crate) fn permit_order_for_test(&self) -> Vec<Option<usize>> {
        self.jobs
            .iter()
            .map(PendingNotificationJob::permit_slot_for_test)
            .collect()
    }
}
