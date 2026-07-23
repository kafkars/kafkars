//! Atomic cancellation outcome ownership after mechanism preflight.

use kafka_client_core::{
    OperationId, ProducerCancellationOutcome as CoreCancellationOutcome, ProducerInput,
    ProducerMachineError,
};

use crate::producer::{ProducerHost, ProducerHostInvariantError};

/// Cancellation resolution exposed only after mechanism and terminal interpretation.
#[derive(Debug)]
pub(in crate::producer) struct ProducerHostCancelAccepted {
    outcome: CoreCancellationOutcome,
}

impl ProducerHostCancelAccepted {
    pub(in crate::producer) const fn outcome(&self) -> CoreCancellationOutcome {
        self.outcome
    }
}

/// Failure before an authoritative cancellation outcome becomes observable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::producer) enum ProducerHostCancelError {
    HostUnavailable(ProducerHostInvariantError),
    ExecutionGenerationExhausted,
    Invariant(ProducerHostInvariantError),
}

impl ProducerHost {
    pub(in crate::producer) fn try_cancel_operation(
        &mut self,
        operation_id: OperationId,
    ) -> Result<ProducerHostCancelAccepted, ProducerHostCancelError> {
        if let Some(error) = self.poison_reason() {
            return Err(ProducerHostCancelError::HostUnavailable(error));
        }
        let preflight = self.preflight_cancellation(operation_id)?;
        let transition = match self
            .core
            .apply(ProducerInput::CancelRequested { operation_id })
        {
            Ok(transition) => transition,
            Err(ProducerMachineError::ExecutionGenerationExhausted) => {
                return if preflight
                    .as_ref()
                    .is_some_and(super::revision::SealedRevisionPlan::generation_exhausted)
                {
                    Err(ProducerHostCancelError::ExecutionGenerationExhausted)
                } else {
                    let invariant = self.poison(ProducerHostInvariantError::Core(
                        ProducerMachineError::ExecutionGenerationExhausted,
                    ));
                    Err(ProducerHostCancelError::Invariant(invariant))
                };
            }
            Err(error) => {
                let invariant = self.poison(ProducerHostInvariantError::Core(error));
                return Err(ProducerHostCancelError::Invariant(invariant));
            }
        };
        let Some(outcome) = transition.cancellation_outcome() else {
            let invariant = self.poison(ProducerHostInvariantError::MissingCancellationOutcome);
            return Err(ProducerHostCancelError::Invariant(invariant));
        };
        if let Err(error) = self.interpret_cancellation_transition(transition, preflight, outcome) {
            let invariant = self.poison(error);
            return Err(ProducerHostCancelError::Invariant(invariant));
        }
        Ok(ProducerHostCancelAccepted { outcome })
    }
}
