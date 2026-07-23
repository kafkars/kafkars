//! Atomic pre-core reservation, deterministic admission, and ownership commit.

use kafka_client_core::{
    Deadline, Moment, OperationId, ProducerCompletion, ProducerInput, ProducerMachineError,
};

use crate::{
    ProducerDeliveryObserver,
    completion::{CompletionId, CompletionObserver},
};

use super::{
    ProducerHost, ProducerHostInvariantError, ProducerRecord, ProducerRejectionReason,
    record_store::RecordReservation,
};

/// Accepted operation identity paired with its sole terminal observer.
#[derive(Debug)]
pub(crate) struct AdmittedExplicit {
    operation_id: OperationId,
    observer: CompletionObserver<ProducerCompletion>,
}

impl AdmittedExplicit {
    pub(crate) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) fn into_delivery_observer(self) -> ProducerDeliveryObserver {
        ProducerDeliveryObserver::from_completion(self.observer)
    }
}

/// Normal admission rejection that preserves the exact caller-owned record.
#[derive(Debug)]
pub(crate) struct RejectedExplicit {
    reason: ProducerRejectionReason,
    record: ProducerRecord,
}

impl RejectedExplicit {
    pub(crate) const fn reason(&self) -> ProducerRejectionReason {
        self.reason
    }

    pub(crate) fn into_record(self) -> ProducerRecord {
        self.record
    }
}

/// Distinguishes ownership-preserving rejection from accepted-state corruption.
#[derive(Debug)]
pub(crate) enum ProducerAdmissionFailure {
    Rejected(RejectedExplicit),
    Invariant(ProducerHostInvariantError),
    AcceptedInvariant(PoisonedExplicit),
}

/// Post-core invariant retaining the sole observer and any known operation ID.
#[derive(Debug)]
pub(crate) struct PoisonedExplicit {
    error: ProducerHostInvariantError,
    operation_id: Option<OperationId>,
    observer: CompletionObserver<ProducerCompletion>,
}

impl PoisonedExplicit {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ProducerHostInvariantError,
        Option<OperationId>,
        CompletionObserver<ProducerCompletion>,
    ) {
        (self.error, self.operation_id, self.observer)
    }
}

impl ProducerHost {
    /// Attempts one explicit-partition admission without blocking or partial ownership.
    #[allow(
        clippy::result_large_err,
        reason = "normal rejection returns the intact linear producer record"
    )]
    pub(crate) fn try_admit_explicit(
        &mut self,
        now: Moment,
        deadline: Deadline,
        record: ProducerRecord,
    ) -> Result<AdmittedExplicit, ProducerAdmissionFailure> {
        if let Some(error) = self.poison_reason() {
            return Err(reject(record, ProducerRejectionReason::HostPoisoned(error)));
        }
        let (completion_id, observer) = match self.completions.reserve() {
            Ok(reserved) => reserved,
            Err(error) => {
                return Err(reject(record, ProducerRejectionReason::Completion(error)));
            }
        };
        let reservation = match self.store.reserve(record) {
            Ok(reservation) => reservation,
            Err(error) => {
                let reason = error.reason();
                let record = error.into_record();
                self.rollback_completion(completion_id, observer)?;
                return Err(reject(record, ProducerRejectionReason::Store(reason)));
            }
        };
        let facts = reservation.facts();
        let transition = match self.core.apply(ProducerInput::AdmitExplicit {
            now,
            deadline,
            record: facts,
        }) {
            Ok(transition) => transition,
            Err(ProducerMachineError::Admission(reason)) => {
                let record = self.rollback_pre_core(completion_id, observer, reservation)?;
                return Err(reject(record, ProducerRejectionReason::Core(reason)));
            }
            Err(error) => {
                let _record = self.rollback_pre_core(completion_id, observer, reservation)?;
                return Err(self.invariant_failure(ProducerHostInvariantError::Core(error)));
            }
        };
        let Some(operation_id) = transition.admitted_operation_id() else {
            return Err(self.accepted_invariant(
                ProducerHostInvariantError::MissingAdmissionIdentity,
                None,
                observer,
            ));
        };
        #[cfg(test)]
        if let Some(error) = self.take_post_acceptance_fault() {
            return Err(self.accepted_invariant(error, Some(operation_id), observer));
        }
        if let Err(error) = self.bindings.bind(operation_id, completion_id) {
            return Err(self.accepted_invariant(
                ProducerHostInvariantError::Binding(error),
                Some(operation_id),
                observer,
            ));
        }
        let committed = match self.store.commit(reservation) {
            Ok(committed) => committed,
            Err(error) => {
                return Err(self.accepted_invariant(
                    ProducerHostInvariantError::Store(error),
                    Some(operation_id),
                    observer,
                ));
            }
        };
        if committed != facts {
            return Err(self.accepted_invariant(
                ProducerHostInvariantError::CommittedFactsMismatch,
                Some(operation_id),
                observer,
            ));
        }
        if let Err(error) = self.interpret_transition(now, transition) {
            return Err(self.accepted_invariant(error, Some(operation_id), observer));
        }
        Ok(AdmittedExplicit {
            operation_id,
            observer,
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "rollback preserves allocation-free ownership-returning admission errors"
    )]
    fn rollback_completion(
        &mut self,
        completion_id: CompletionId,
        observer: CompletionObserver<ProducerCompletion>,
    ) -> Result<(), ProducerAdmissionFailure> {
        let result = self.completions.rollback_reservation(completion_id);
        drop(observer);
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                Err(self.invariant_failure(ProducerHostInvariantError::Completion(error)))
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "rollback preserves allocation-free ownership-returning admission errors"
    )]
    fn rollback_pre_core(
        &mut self,
        completion_id: CompletionId,
        observer: CompletionObserver<ProducerCompletion>,
        reservation: RecordReservation,
    ) -> Result<ProducerRecord, ProducerAdmissionFailure> {
        let completion_result = self.completions.rollback_reservation(completion_id);
        let record_result = self.store.rollback(reservation);
        drop(observer);
        if let Err(error) = completion_result {
            return Err(self.invariant_failure(ProducerHostInvariantError::Completion(error)));
        }
        match record_result {
            Ok(record) => Ok(record),
            Err(error) => Err(self.invariant_failure(ProducerHostInvariantError::Store(error))),
        }
    }

    fn invariant_failure(&mut self, error: ProducerHostInvariantError) -> ProducerAdmissionFailure {
        ProducerAdmissionFailure::Invariant(self.poison(error))
    }

    fn accepted_invariant(
        &mut self,
        error: ProducerHostInvariantError,
        operation_id: Option<OperationId>,
        observer: CompletionObserver<ProducerCompletion>,
    ) -> ProducerAdmissionFailure {
        ProducerAdmissionFailure::AcceptedInvariant(PoisonedExplicit {
            error: self.poison(error),
            operation_id,
            observer,
        })
    }
}

fn reject(record: ProducerRecord, reason: ProducerRejectionReason) -> ProducerAdmissionFailure {
    ProducerAdmissionFailure::Rejected(RejectedExplicit { reason, record })
}
