//! Atomic pre-core rollback and recoverable record-ownership retention.

use kafka_client_core::ProducerCompletion;

use crate::completion::{CompletionId, CompletionObserver};

use super::{
    super::{
        ProducerHost, ProducerHostInvariantError, ProducerRecord, record_store::RecordReservation,
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
        observer: CompletionObserver<ProducerCompletion>,
        record: ProducerRecord,
    ) -> Result<ProducerRecord, ProducerAdmissionFailure> {
        let result = self.completions.rollback_reservation(completion_id);
        drop(observer);
        match result {
            Ok(()) => Ok(record),
            Err(error) => {
                Err(self
                    .invariant_failure(ProducerHostInvariantError::Completion(error), Some(record)))
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
        observer: CompletionObserver<ProducerCompletion>,
        reservation: RecordReservation,
    ) -> Result<ProducerRecord, ProducerAdmissionFailure> {
        let completion_result = self.completions.rollback_reservation(completion_id);
        let record_result = self.store.rollback(reservation);
        drop(observer);
        match (completion_result, record_result) {
            (Ok(()), Ok(record)) => Ok(record),
            (Err(error), Ok(record)) => {
                Err(self
                    .invariant_failure(ProducerHostInvariantError::Completion(error), Some(record)))
            }
            (Ok(()), Err(error)) => {
                Err(self.invariant_failure(ProducerHostInvariantError::Store(error), None))
            }
            (Err(error), Err(_store_error)) => {
                Err(self.invariant_failure(ProducerHostInvariantError::Completion(error), None))
            }
        }
    }

    pub(super) fn invariant_failure(
        &mut self,
        error: ProducerHostInvariantError,
        record: Option<ProducerRecord>,
    ) -> ProducerAdmissionFailure {
        ProducerAdmissionFailure::Invariant(PoisonedBeforeOwnership {
            error: self.poison(error),
            record,
        })
    }
}
