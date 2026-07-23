//! Atomic pre-core rollback and recoverable record-ownership retention.

use crate::completion::{CompletionId, CompletionObserver};

use super::{
    super::{
        ProducerHost, ProducerHostInvariantError, ProducerRecord, record_store::RecordReservation,
        terminal::ProducerTerminal,
    },
    PoisonedBeforeOwnership, ProducerAdmissionFailure,
};

impl ProducerHost {
    #[allow(
        clippy::result_large_err,
        reason = "rollback preserves allocation-free ownership-returning admission errors"
    )]
    pub(super) fn rollback_completion(
        &mut self,
        completion_id: CompletionId,
        observer: CompletionObserver<ProducerTerminal>,
        record: ProducerRecord,
    ) -> Result<ProducerRecord, ProducerAdmissionFailure> {
        let result = self.completions.rollback_reservation(completion_id);
        drop(observer);
        match result {
            Ok(()) => Ok(record),
            Err(error) => {
                Err(self.invariant_failure(ProducerHostInvariantError::Completion(error), record))
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "rollback preserves allocation-free ownership-returning admission errors"
    )]
    pub(super) fn rollback_pre_core(
        &mut self,
        completion_id: CompletionId,
        observer: CompletionObserver<ProducerTerminal>,
        reservation: RecordReservation,
    ) -> Result<ProducerRecord, ProducerAdmissionFailure> {
        let completion_result = self.completions.rollback_reservation(completion_id);
        let (record, store_result) = self.store.rollback(reservation).into_parts();
        drop(observer);
        match (completion_result, store_result) {
            (Ok(()), Ok(())) => Ok(record),
            (Err(error), Ok(()) | Err(_)) => {
                Err(self.invariant_failure(ProducerHostInvariantError::Completion(error), record))
            }
            (Ok(()), Err(error)) => {
                Err(self.invariant_failure(ProducerHostInvariantError::Store(error), record))
            }
        }
    }

    pub(super) fn invariant_failure(
        &mut self,
        error: ProducerHostInvariantError,
        record: ProducerRecord,
    ) -> ProducerAdmissionFailure {
        ProducerAdmissionFailure::Invariant(PoisonedBeforeOwnership {
            error: self.poison(error),
            record,
        })
    }
}
