//! Dormant ownership-complete settlement of one FIFO pending promotion.

use kafka_client_core::Moment;

use super::{
    data::{ProducerShardAdmission, ProducerShardData},
    pending_fatal::{
        PendingAcceptedInvariant, PendingNotificationContext, PendingShardFatal,
        PendingShardFatalRetentionFailure, PendingStartInvariant,
    },
    promotion_error::{
        PendingPromotionFailure, PendingPromotionResolution, PendingStartResolution,
    },
};

/// Scheduler meaning of one bounded pending-settlement attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingSettlementDisposition {
    Idle,
    Productive,
    RestoredBlocked,
    Faulted,
}

/// Copy-only facts left after every linear promotion owner was consumed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingSettlementProgress {
    inspected: usize,
    remaining: bool,
    disposition: PendingSettlementDisposition,
}

enum NotificationRetention {
    Retained,
    Faulted(PendingSettlementProgress),
}
impl PendingSettlementProgress {
    pub(crate) const fn inspected(self) -> usize {
        self.inspected
    }

    pub(crate) const fn remaining(self) -> bool {
        self.remaining
    }

    pub(crate) const fn disposition(self) -> PendingSettlementDisposition {
        self.disposition
    }
}
impl ProducerShardData {
    /// Consumes every owner produced by one dormant FIFO promotion attempt.
    pub(crate) fn settle_next_pending(
        &mut self,
        now: Moment,
    ) -> Result<PendingSettlementProgress, PendingShardFatalRetentionFailure> {
        let inactive = match &self.admission {
            ProducerShardAdmission::Running => None,
            ProducerShardAdmission::Closed => Some(PendingSettlementDisposition::Idle),
            ProducerShardAdmission::Faulted(_) => Some(PendingSettlementDisposition::Faulted),
        };
        if let Some(disposition) = inactive {
            return Ok(self.progress(0, disposition));
        }
        let promotion = match self.promote_next(now) {
            Ok(promotion) => promotion,
            Err(PendingPromotionFailure::Closed) => {
                return Ok(self.progress(0, PendingSettlementDisposition::Idle));
            }
            Err(failure) => {
                let inspected = failure_inspected(&failure);
                return retain_fatal(self, inspected, PendingShardFatal::promotion(failure));
            }
        };
        let inspected = promotion.inspected();
        let Some(resolution) = promotion.into_resolution() else {
            let disposition = if inspected == 0 {
                PendingSettlementDisposition::Idle
            } else {
                PendingSettlementDisposition::Productive
            };
            return Ok(self.progress(inspected, disposition));
        };
        settle_resolution(self, inspected, resolution)
    }

    fn progress(
        &self,
        inspected: usize,
        disposition: PendingSettlementDisposition,
    ) -> PendingSettlementProgress {
        PendingSettlementProgress {
            inspected,
            remaining: self.pending.has_entries(),
            disposition,
        }
    }

    #[cfg(test)]
    pub(crate) fn retain_promotion_failure_for_test(
        &mut self,
        failure: PendingPromotionFailure,
    ) -> Result<PendingSettlementProgress, PendingShardFatalRetentionFailure> {
        let inspected = failure_inspected(&failure);
        retain_fatal(self, inspected, PendingShardFatal::promotion(failure))
    }
}

fn settle_resolution(
    data: &mut ProducerShardData,
    inspected: usize,
    resolution: PendingPromotionResolution,
) -> Result<PendingSettlementProgress, PendingShardFatalRetentionFailure> {
    match resolution {
        PendingPromotionResolution::Accepted(accepted) => {
            let (operation_id, job, invariant) = accepted.into_parts();
            let context = invariant.map_or(
                PendingNotificationContext::Accepted { operation_id },
                |invariant| {
                    PendingNotificationContext::AcceptedInvariant(PendingAcceptedInvariant {
                        operation_id,
                        invariant,
                    })
                },
            );
            if let NotificationRetention::Faulted(progress) =
                retain_notification(data, inspected, context, job)?
            {
                return Ok(progress);
            }
            match invariant {
                Some(invariant) => retain_fatal(
                    data,
                    inspected,
                    PendingShardFatal::accepted_invariant(operation_id, invariant),
                ),
                None => Ok(data.progress(inspected, PendingSettlementDisposition::Productive)),
            }
        }
        PendingPromotionResolution::Restored => {
            Ok(data.progress(inspected, PendingSettlementDisposition::RestoredBlocked))
        }
        PendingPromotionResolution::Abandoned(admission) => {
            drop(admission);
            Ok(data.progress(inspected, PendingSettlementDisposition::Productive))
        }
        PendingPromotionResolution::Local(local) => {
            let failure = local.failure();
            let (admission, job) = local.into_parts();
            drop(admission);
            if let NotificationRetention::Faulted(progress) = retain_notification(
                data,
                inspected,
                PendingNotificationContext::Local(failure),
                job,
            )? {
                return Ok(progress);
            }
            Ok(data.progress(inspected, PendingSettlementDisposition::Productive))
        }
        PendingPromotionResolution::Start(start) => settle_start(data, inspected, start),
    }
}

fn settle_start(
    data: &mut ProducerShardData,
    inspected: usize,
    start: PendingStartResolution,
) -> Result<PendingSettlementProgress, PendingShardFatalRetentionFailure> {
    let (failure, invariant) = start.into_parts();
    let failure_fact = failure.failure();
    let (admission, job) = failure.into_parts();
    drop(admission);
    let context = invariant.map_or(
        PendingNotificationContext::Start(failure_fact),
        |invariant| {
            PendingNotificationContext::StartInvariant(PendingStartInvariant {
                failure: failure_fact,
                invariant,
            })
        },
    );
    if let NotificationRetention::Faulted(progress) =
        retain_notification(data, inspected, context, job)?
    {
        return Ok(progress);
    }
    match invariant {
        Some(invariant) => retain_fatal(
            data,
            inspected,
            PendingShardFatal::start_invariant(failure_fact, invariant),
        ),
        None => Ok(data.progress(inspected, PendingSettlementDisposition::Productive)),
    }
}

fn retain_notification(
    data: &mut ProducerShardData,
    inspected: usize,
    context: PendingNotificationContext,
    job: crate::producer::pending::PendingNotificationJob,
) -> Result<NotificationRetention, PendingShardFatalRetentionFailure> {
    match data
        .host
        .pending_notifications
        .retain_pending_notification(job)
    {
        Ok(_mode) => Ok(NotificationRetention::Retained),
        Err(refused) => retain_fatal(
            data,
            inspected,
            PendingShardFatal::notification(context, refused.into_job()),
        )
        .map(NotificationRetention::Faulted),
    }
}

fn retain_fatal(
    data: &mut ProducerShardData,
    inspected: usize,
    fatal: PendingShardFatal,
) -> Result<PendingSettlementProgress, PendingShardFatalRetentionFailure> {
    data.retain_pending_fatal(fatal)?;
    Ok(data.progress(inspected, PendingSettlementDisposition::Faulted))
}

const fn failure_inspected(failure: &PendingPromotionFailure) -> usize {
    match failure {
        PendingPromotionFailure::Closed => 0,
        PendingPromotionFailure::Take(failure) => failure.inspected(),
        PendingPromotionFailure::Detach { .. }
        | PendingPromotionFailure::RecordRestore { .. }
        | PendingPromotionFailure::Restore(_)
        | PendingPromotionFailure::AcceptedCommit(_)
        | PendingPromotionFailure::Accept { .. }
        | PendingPromotionFailure::Local(_)
        | PendingPromotionFailure::Start(_)
        | PendingPromotionFailure::Fatal { .. } => 1,
    }
}
