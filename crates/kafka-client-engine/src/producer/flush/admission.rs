//! Atomic completion reservation and deterministic producer flush admission.

use kafka_client_core::{FlushId, FlushLedgerError, Moment, ProducerInput, ProducerMachineError};

use crate::ProducerFlushObserver;
use crate::completion::{CompletionId, CompletionObserver, CompletionRegistryError};

use super::super::{ProducerHost, ProducerHostInvariantError, terminal::ProducerTerminal};

/// Accepted flush identity paired with its sole terminal observer.
#[derive(Debug)]
pub(crate) struct AdmittedFlush {
    flush_id: FlushId,
    observer: CompletionObserver<ProducerTerminal>,
}

impl AdmittedFlush {
    pub(crate) const fn flush_id(&self) -> FlushId {
        self.flush_id
    }

    pub(crate) fn into_flush_observer(self) -> ProducerFlushObserver {
        ProducerFlushObserver::from_completion(self.observer)
    }
}

/// Healthy flush rejection before core ownership crossed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FlushRejectionReason {
    Completion(CompletionRegistryError),
    Core(FlushLedgerError),
    Closed,
    HostPoisoned(ProducerHostInvariantError),
}

/// Distinguishes healthy flush rejection from pre- and post-acceptance damage.
#[derive(Debug)]
pub(crate) enum FlushAdmissionFailure {
    Rejected(FlushRejectionReason),
    Invariant(ProducerHostInvariantError),
    AcceptedInvariant {
        error: ProducerHostInvariantError,
        flush_id: Option<FlushId>,
        observer: CompletionObserver<ProducerTerminal>,
    },
}

impl ProducerHost {
    /// Reserves terminal capacity before core can accept one flush barrier.
    pub(crate) fn try_admit_flush(
        &mut self,
        now: Moment,
    ) -> Result<AdmittedFlush, FlushAdmissionFailure> {
        self.try_admit_barrier(now, ProducerInput::FlushRequested)
    }

    /// Reserves terminal capacity before core can close admission and accept its drain barrier.
    pub(crate) fn try_admit_close(
        &mut self,
        now: Moment,
    ) -> Result<AdmittedFlush, FlushAdmissionFailure> {
        self.try_admit_barrier(now, ProducerInput::CloseRequested)
    }

    fn try_admit_barrier(
        &mut self,
        now: Moment,
        request: ProducerInput,
    ) -> Result<AdmittedFlush, FlushAdmissionFailure> {
        if let Some(error) = self.poison_reason() {
            return Err(FlushAdmissionFailure::Rejected(
                FlushRejectionReason::HostPoisoned(error),
            ));
        }
        let (completion_id, observer) = self.completions.reserve().map_err(|error| {
            FlushAdmissionFailure::Rejected(FlushRejectionReason::Completion(error))
        })?;
        let transition = match self.core.apply(request) {
            Ok(transition) => transition,
            Err(ProducerMachineError::Flush(error)) => {
                self.rollback_flush_reservation(completion_id, observer)?;
                return Err(FlushAdmissionFailure::Rejected(FlushRejectionReason::Core(
                    error,
                )));
            }
            Err(error) => {
                self.rollback_flush_reservation(completion_id, observer)?;
                return Err(FlushAdmissionFailure::Invariant(
                    self.poison(ProducerHostInvariantError::Core(error)),
                ));
            }
        };
        let Some(flush_id) = transition.accepted_flush_id() else {
            return Err(self.accepted_flush_invariant(
                ProducerHostInvariantError::MissingFlushIdentity,
                None,
                observer,
            ));
        };
        if let Err(error) = self.flush_bindings.bind(flush_id, completion_id) {
            return Err(self.accepted_flush_invariant(
                ProducerHostInvariantError::FlushBinding(error),
                Some(flush_id),
                observer,
            ));
        }
        if let Err(error) = self.interpret_transition(now, &transition) {
            return Err(self.accepted_flush_invariant(error, Some(flush_id), observer));
        }
        Ok(AdmittedFlush { flush_id, observer })
    }

    fn rollback_flush_reservation(
        &mut self,
        completion_id: CompletionId,
        observer: CompletionObserver<ProducerTerminal>,
    ) -> Result<(), FlushAdmissionFailure> {
        let result = self.completions.rollback_reservation(completion_id);
        drop(observer);
        result.map_err(|error| {
            FlushAdmissionFailure::Invariant(
                self.poison(ProducerHostInvariantError::Completion(error)),
            )
        })
    }

    fn accepted_flush_invariant(
        &mut self,
        error: ProducerHostInvariantError,
        flush_id: Option<FlushId>,
        observer: CompletionObserver<ProducerTerminal>,
    ) -> FlushAdmissionFailure {
        FlushAdmissionFailure::AcceptedInvariant {
            error: self.poison(error),
            flush_id,
            observer,
        }
    }
}
