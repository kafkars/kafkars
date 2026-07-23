//! Bounded FIFO retention and off-reactor recovery of pending notification jobs.

use std::collections::VecDeque;

use super::{PendingNotificationDispatchAuthority, PendingNotificationJob};

#[path = "backlog/authority.rs"]
mod authority;
pub(super) use authority::PendingNotificationRecoveryDispatchOwner;

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

    /// Transfers older retained jobs plus the exact closed-queue return.
    pub(crate) fn into_recovery(
        self,
        returned: PendingNotificationJob,
    ) -> PendingNotificationRecovery {
        PendingNotificationRecovery {
            jobs: self.jobs,
            returned: Some(returned),
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.jobs.len()
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
    returned: Option<PendingNotificationJob>,
}

impl PendingNotificationRecovery {
    /// Dispatches retained FIFO work and then the exact closed-queue return.
    ///
    /// This loop is private so reactor owners cannot invoke it with a recovery
    /// value. A production worker handoff must be added before recovery is
    /// connected to the host.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "off-reactor worker handoff precedes production recovery integration"
        )
    )]
    fn run_off_reactor(mut self) {
        let authority = PendingNotificationDispatchAuthority::from_recovery(
            PendingNotificationRecoveryDispatchOwner::new(),
        );
        while let Some(job) = self.jobs.pop_front() {
            job.dispatch_pending_notification(&authority);
        }
        if let Some(returned) = self.returned.take() {
            returned.dispatch_pending_notification(&authority);
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
            .chain(
                self.returned
                    .as_ref()
                    .map(PendingNotificationJob::permit_slot_for_test),
            )
            .collect()
    }
}
