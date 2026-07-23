//! Exact record transfer state owned by one coordinated promotion attempt.

use crate::producer::ProducerRecord;

use super::{PendingAttemptStateError, PendingPromotionAttempt};

/// Record-transfer phase inside one coordinated promotion owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingRecordTransferState {
    Retained,
    Detached,
    Committed,
}

impl PendingPromotionAttempt {
    /// Detaches the exact record only for the producer admission friend.
    pub(in crate::producer) fn detach_record(
        &mut self,
    ) -> Result<ProducerRecord, PendingAttemptStateError> {
        if self.transfer != PendingRecordTransferState::Retained {
            return Err(PendingAttemptStateError::RecordNotRetained);
        }
        let Some(admission) = self.admission.take() else {
            return Err(PendingAttemptStateError::Invariant);
        };
        let (facts, record) = admission.into_transfer_parts();
        self.facts = Some(facts);
        self.transfer = PendingRecordTransferState::Detached;
        Ok(record)
    }

    /// Restores an exact record returned by healthy admission rejection.
    pub(in crate::producer) fn restore_record(
        &mut self,
        record: ProducerRecord,
    ) -> Result<(), PendingRecordRestoreFailure> {
        if self.transfer != PendingRecordTransferState::Detached {
            return Err(PendingRecordRestoreFailure::new(
                PendingAttemptStateError::RecordNotDetached,
                record,
            ));
        }
        let Some(facts) = self.facts.take() else {
            return Err(PendingRecordRestoreFailure::new(
                PendingAttemptStateError::Invariant,
                record,
            ));
        };
        self.admission = Some(facts.restore(record));
        self.transfer = PendingRecordTransferState::Retained;
        Ok(())
    }

    /// Records that the detached bytes crossed deterministic admission.
    pub(in crate::producer) fn commit_record(&mut self) -> Result<(), PendingAttemptStateError> {
        if self.transfer != PendingRecordTransferState::Detached {
            return Err(PendingAttemptStateError::RecordNotDetached);
        }
        if self.facts.is_none() {
            return Err(PendingAttemptStateError::Invariant);
        }
        self.transfer = PendingRecordTransferState::Committed;
        Ok(())
    }

    pub(crate) const fn transfer_state(&self) -> PendingRecordTransferState {
        self.transfer
    }
}

/// Failed restoration retaining the exact detached record.
#[must_use = "the detached record remains owned by this failure"]
pub(crate) struct PendingRecordRestoreFailure {
    error: PendingAttemptStateError,
    record: Box<ProducerRecord>,
}

impl PendingRecordRestoreFailure {
    fn new(error: PendingAttemptStateError, record: ProducerRecord) -> Self {
        Self {
            error,
            record: Box::new(record),
        }
    }

    pub(crate) fn into_parts(self) -> (PendingAttemptStateError, ProducerRecord) {
        (self.error, *self.record)
    }
}
