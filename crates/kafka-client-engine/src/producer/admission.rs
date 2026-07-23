//! Atomic pre-core reservation, deterministic admission, and ownership commit.

use kafka_client_core::{Moment, OperationId, ProducerInput, ProducerMachineError};

use crate::{ProducerDeliveryObserver, clock::OperationDeadline, completion::CompletionObserver};

use super::{
    ProducerHost, ProducerHostInvariantError, ProducerRecord, ProducerRejectionReason,
    terminal::ProducerTerminal,
};

mod rollback;

/// Accepted operation identity paired with its sole terminal observer.
#[derive(Debug)]
pub(crate) struct AdmittedExplicit {
    operation_id: OperationId,
    observer: CompletionObserver<ProducerTerminal>,
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
    Invariant(PoisonedBeforeOwnership),
    AcceptedInvariant(PoisonedExplicit),
}

/// Pre-core poison retaining exact record ownership despite cleanup failure.
#[derive(Debug)]
pub(crate) struct PoisonedBeforeOwnership {
    error: ProducerHostInvariantError,
    record: ProducerRecord,
}

impl PoisonedBeforeOwnership {
    pub(crate) fn into_parts(self) -> (ProducerHostInvariantError, ProducerRecord) {
        (self.error, self.record)
    }
}

/// Post-core invariant retaining the sole observer and any known operation ID.
#[derive(Debug)]
pub(crate) struct PoisonedExplicit {
    error: ProducerHostInvariantError,
    operation_id: Option<OperationId>,
    observer: CompletionObserver<ProducerTerminal>,
}

impl PoisonedExplicit {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ProducerHostInvariantError,
        Option<OperationId>,
        CompletionObserver<ProducerTerminal>,
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
        deadline: OperationDeadline,
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
                let record = self.rollback_completion(completion_id, observer, record)?;
                return Err(reject(record, ProducerRejectionReason::Store(reason)));
            }
        };
        let facts = reservation.facts();
        let transition = match self.core.apply(ProducerInput::AdmitExplicit {
            now,
            deadline: deadline.core(),
            record: facts,
        }) {
            Ok(transition) => transition,
            Err(ProducerMachineError::Admission(reason)) => {
                let record = self.rollback_pre_core(completion_id, observer, reservation)?;
                return Err(reject(record, ProducerRejectionReason::Core(reason)));
            }
            Err(error) => {
                let record = self.rollback_pre_core(completion_id, observer, reservation)?;
                return Err(self.invariant_failure(ProducerHostInvariantError::Core(error), record));
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
        if let Err(error) = self.bindings.bind(operation_id, completion_id, deadline) {
            return Err(self.accepted_invariant(
                ProducerHostInvariantError::Binding(error),
                Some(operation_id),
                observer,
            ));
        }
        // Core ownership crossed above. A store disagreement from this point
        // remains an accepted invariant with the sole terminal observer; it
        // can never become a record-returning pre-ownership rejection.
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

    fn accepted_invariant(
        &mut self,
        error: ProducerHostInvariantError,
        operation_id: Option<OperationId>,
        observer: CompletionObserver<ProducerTerminal>,
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
