//! Linear first-fault retention for dormant pending-promotion recovery.

use super::{
    data::{ProducerShardAdmission, ProducerShardData},
    promotion_error::PendingPromotionFailure,
};

/// Exact pending-promotion owner retained after shard admission is closed.
#[must_use = "the first shard fault owns exact pending-promotion recovery state"]
pub(crate) struct PendingShardFatal {
    failure: PendingPromotionFailure,
}

impl PendingShardFatal {
    pub(crate) const fn new(failure: PendingPromotionFailure) -> Self {
        Self { failure }
    }

    #[cfg(test)]
    pub(crate) const fn failure_for_test(&self) -> &PendingPromotionFailure {
        &self.failure
    }
}

/// Refusal preserving a later fault when the first owner already won.
#[must_use = "the refused fault remains an exact linear recovery owner"]
pub(crate) struct PendingShardFatalRetentionFailure {
    incoming: PendingShardFatal,
}

impl PendingShardFatalRetentionFailure {
    pub(crate) fn into_owner(self) -> PendingShardFatal {
        self.incoming
    }
}

impl ProducerShardData {
    /// Closes both admission domains before installing the immutable first fault.
    #[allow(
        dead_code,
        reason = "live pending settlement will supply the first fault in a later slice"
    )]
    pub(crate) fn retain_pending_fatal(
        &mut self,
        incoming: PendingShardFatal,
    ) -> Result<(), PendingShardFatalRetentionFailure> {
        if !matches!(&self.admission, ProducerShardAdmission::Running) {
            return Err(PendingShardFatalRetentionFailure { incoming });
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
