//! Dormant bounded expiry and shutdown drain for pending producer sends.

use std::num::NonZeroUsize;

use kafka_client_core::{Deadline, Moment};

use crate::producer::pending::{
    PendingLocalFailure, PendingNotificationJob, turn_error::PendingTurnFailureOwnership,
};

use super::{
    data::{ProducerShardAdmission, ProducerShardData},
    pending_local_fatal::{
        PendingLocalSettlementFatal, PendingLocalSettlementMode,
        PendingLocalSettlementRetentionFailure,
    },
};

/// Scheduler meaning of one bounded pending-local settlement turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingLocalSettlementDisposition {
    Expiry,
    ShutdownDrain,
    Faulted,
}

/// Copy facts needed to schedule the next pending-local turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingLocalSettlementProgress {
    disposition: PendingLocalSettlementDisposition,
    inspected: usize,
    notifications_retained: usize,
    pending_owned: bool,
    runnable: bool,
    next_deadline: Option<Deadline>,
    route_pending: usize,
}

impl PendingLocalSettlementProgress {
    pub(crate) const fn disposition(self) -> PendingLocalSettlementDisposition {
        self.disposition
    }

    pub(crate) const fn inspected(self) -> usize {
        self.inspected
    }

    pub(crate) const fn notifications_retained(self) -> usize {
        self.notifications_retained
    }

    pub(crate) const fn pending_owned(self) -> bool {
        self.pending_owned
    }

    pub(crate) const fn runnable(self) -> bool {
        self.runnable
    }

    pub(crate) const fn next_deadline(self) -> Option<Deadline> {
        self.next_deadline
    }

    pub(crate) const fn route_pending(self) -> usize {
        self.route_pending
    }
}

impl ProducerShardData {
    /// Settles one caller-bounded local turn without dispatch or live scheduling.
    #[allow(
        clippy::result_large_err,
        reason = "a later failure returns exact ownership without allocating after route refusal"
    )]
    pub(crate) fn settle_pending_local(
        &mut self,
        now: Moment,
        limit: NonZeroUsize,
    ) -> Result<PendingLocalSettlementProgress, PendingLocalSettlementRetentionFailure> {
        let mode = match &self.admission {
            ProducerShardAdmission::Running => PendingLocalSettlementMode::Expiry,
            ProducerShardAdmission::Closed => PendingLocalSettlementMode::ShutdownDrain,
            ProducerShardAdmission::Faulted(_) => {
                return Ok(self.local_progress(
                    PendingLocalSettlementDisposition::Faulted,
                    0,
                    0,
                    false,
                ));
            }
        };
        let turn = match mode {
            PendingLocalSettlementMode::Expiry => self.pending.expire_due(now, limit.get()),
            PendingLocalSettlementMode::ShutdownDrain => self.pending.drain_closed(limit.get()),
        };
        match turn {
            Ok(progress) => {
                let inspected = progress.inspected();
                let runnable = progress.remaining();
                route_failures(
                    self,
                    mode,
                    inspected,
                    runnable,
                    progress.into_failures(),
                    None,
                )
            }
            Err(failure) => {
                let inspected = failure.inspected();
                let (completed, source) = failure.into_parts();
                route_failures(self, mode, inspected, false, completed, Some(source))
            }
        }
    }

    fn local_progress(
        &self,
        disposition: PendingLocalSettlementDisposition,
        inspected: usize,
        notifications_retained: usize,
        runnable: bool,
    ) -> PendingLocalSettlementProgress {
        PendingLocalSettlementProgress {
            disposition,
            inspected,
            notifications_retained,
            pending_owned: self.pending.has_entries() || self.has_pending_fatal(),
            runnable,
            next_deadline: self.pending.next_deadline(),
            route_pending: self.host.pending_notifications.retained_len(),
        }
    }
}

#[allow(
    clippy::result_large_err,
    reason = "refusal returns exact local ownership without allocating after route failure"
)]
fn route_failures(
    data: &mut ProducerShardData,
    mode: PendingLocalSettlementMode,
    inspected: usize,
    runnable: bool,
    mut failures: Vec<PendingLocalFailure>,
    source: Option<PendingTurnFailureOwnership>,
) -> Result<PendingLocalSettlementProgress, PendingLocalSettlementRetentionFailure> {
    failures.reverse();
    let mut retained = 0;
    while let Some(failure) = failures.pop() {
        let failure_fact = failure.failure();
        let (pending, job) = failure.into_parts();
        drop(pending);
        match retain(data, job) {
            Ok(()) => retained += 1,
            Err(refused) => {
                failures.reverse();
                let fatal = PendingLocalSettlementFatal::route_refusal(
                    mode,
                    inspected,
                    retained,
                    failure_fact,
                    refused,
                    failures,
                    source,
                );
                return retain_fatal(data, fatal, inspected, retained);
            }
        }
    }
    if let Some(source) = source {
        let fatal = PendingLocalSettlementFatal::source_failure(mode, inspected, retained, source);
        return retain_fatal(data, fatal, inspected, retained);
    }
    Ok(data.local_progress(disposition(mode), inspected, retained, runnable))
}

fn retain(
    data: &mut ProducerShardData,
    job: PendingNotificationJob,
) -> Result<(), PendingNotificationJob> {
    match data
        .host
        .pending_notifications
        .retain_pending_notification(job)
    {
        Ok(_mode) => Ok(()),
        Err(refused) => Err(refused.into_job()),
    }
}

#[allow(
    clippy::result_large_err,
    reason = "refusal returns exact local ownership without allocating after route failure"
)]
fn retain_fatal(
    data: &mut ProducerShardData,
    fatal: PendingLocalSettlementFatal,
    inspected: usize,
    retained: usize,
) -> Result<PendingLocalSettlementProgress, PendingLocalSettlementRetentionFailure> {
    data.retain_pending_local_fatal(fatal)?;
    Ok(data.local_progress(
        PendingLocalSettlementDisposition::Faulted,
        inspected,
        retained,
        false,
    ))
}

const fn disposition(mode: PendingLocalSettlementMode) -> PendingLocalSettlementDisposition {
    match mode {
        PendingLocalSettlementMode::Expiry => PendingLocalSettlementDisposition::Expiry,
        PendingLocalSettlementMode::ShutdownDrain => {
            PendingLocalSettlementDisposition::ShutdownDrain
        }
    }
}
