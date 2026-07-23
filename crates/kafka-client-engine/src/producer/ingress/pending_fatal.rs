//! Linear first-fault retention for dormant pending-promotion settlement.

use kafka_client_core::OperationId;

use crate::{
    ProducerSendStartFailure,
    producer::pending::{PendingNotificationJob, ProducerSendFailure},
};

use super::{
    data::{ProducerShardAdmission, ProducerShardData},
    promotion_error::{PendingPromotionFailure, PendingPromotionInvariant},
};

/// Copy context that prevents a refused notification from changing semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingNotificationContext {
    Accepted { operation_id: Option<OperationId> },
    Local(ProducerSendFailure),
    Start(ProducerSendStartFailure),
    AcceptedInvariant(PendingAcceptedInvariant),
    StartInvariant(PendingStartInvariant),
}

/// Post-acceptance diagnostic retained only after observation is preserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingAcceptedInvariant {
    pub(crate) operation_id: Option<OperationId>,
    pub(crate) invariant: PendingPromotionInvariant,
}

/// Pre-admission diagnostic retained only after local settlement is preserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingStartInvariant {
    pub(crate) failure: ProducerSendStartFailure,
    pub(crate) invariant: PendingPromotionInvariant,
}

/// Exact notification owner returned by a bounded route disagreement.
#[must_use = "the refused job still owns its pending notification permit"]
pub(crate) struct PendingNotificationFatal {
    context: PendingNotificationContext,
    job: PendingNotificationJob,
}

impl PendingNotificationFatal {
    pub(crate) const fn new(
        context: PendingNotificationContext,
        job: PendingNotificationJob,
    ) -> Self {
        Self { context, job }
    }

    #[cfg(test)]
    pub(crate) const fn context_for_test(&self) -> PendingNotificationContext {
        self.context
    }

    #[cfg(test)]
    pub(crate) const fn permit_slot_for_test(&self) -> Option<usize> {
        self.job.permit_slot_for_test()
    }
}

/// Exact pending owner retained after shard admission is closed.
#[must_use = "the first shard fault owns exact pending-promotion recovery state"]
pub(crate) enum PendingShardFatal {
    Promotion(PendingPromotionFailure),
    Notification(PendingNotificationFatal),
    AcceptedInvariant(PendingAcceptedInvariant),
    StartInvariant(PendingStartInvariant),
}

impl PendingShardFatal {
    pub(crate) const fn promotion(failure: PendingPromotionFailure) -> Self {
        Self::Promotion(failure)
    }

    pub(crate) const fn notification(
        context: PendingNotificationContext,
        job: PendingNotificationJob,
    ) -> Self {
        Self::Notification(PendingNotificationFatal::new(context, job))
    }

    pub(crate) const fn accepted_invariant(
        operation_id: Option<OperationId>,
        invariant: PendingPromotionInvariant,
    ) -> Self {
        Self::AcceptedInvariant(PendingAcceptedInvariant {
            operation_id,
            invariant,
        })
    }

    pub(crate) const fn start_invariant(
        failure: ProducerSendStartFailure,
        invariant: PendingPromotionInvariant,
    ) -> Self {
        Self::StartInvariant(PendingStartInvariant { failure, invariant })
    }

    #[cfg(test)]
    pub(crate) const fn promotion_for_test(&self) -> Option<&PendingPromotionFailure> {
        match self {
            Self::Promotion(failure) => Some(failure),
            Self::Notification(_) | Self::AcceptedInvariant(_) | Self::StartInvariant(_) => None,
        }
    }
}

/// Refusal preserving a later fault when the first owner already won.
#[must_use = "the refused fault remains an exact linear recovery owner"]
pub(crate) struct PendingShardFatalRetentionFailure {
    incoming: Box<PendingShardFatal>,
}

impl PendingShardFatalRetentionFailure {
    pub(crate) fn into_owner(self) -> PendingShardFatal {
        *self.incoming
    }
}

impl ProducerShardData {
    /// Closes both admission domains before installing the immutable first fault.
    pub(crate) fn retain_pending_fatal(
        &mut self,
        incoming: PendingShardFatal,
    ) -> Result<(), PendingShardFatalRetentionFailure> {
        if !matches!(&self.admission, ProducerShardAdmission::Running) {
            return Err(PendingShardFatalRetentionFailure {
                incoming: Box::new(incoming),
            });
        }
        self.pending.begin_close();
        self.host.close_admission();
        self.admission = ProducerShardAdmission::Faulted(incoming);
        Ok(())
    }

    pub(super) const fn has_pending_fatal(&self) -> bool {
        matches!(&self.admission, ProducerShardAdmission::Faulted(_))
    }

    #[cfg(test)]
    pub(crate) const fn pending_fatal_for_test(&self) -> Option<&PendingShardFatal> {
        match &self.admission {
            ProducerShardAdmission::Faulted(fatal) => Some(fatal),
            ProducerShardAdmission::Running | ProducerShardAdmission::Closed => None,
        }
    }
}
