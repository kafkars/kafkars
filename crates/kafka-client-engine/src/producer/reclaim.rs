//! Two-phase core and engine ownership of terminal completion reclamation.

use std::{error::Error, fmt};

use kafka_client_core::{OperationId, ProducerCompletion, ProducerInput};

use crate::completion::{CompletionId, CompletionRegistry, CompletionRegistryError, ReclaimStatus};

use super::binding::{OperationBindingError, OperationBindings};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReclaimPhase {
    Idle,
    AwaitingCore {
        completion_id: CompletionId,
        operation_id: OperationId,
    },
    Finishing {
        completion_id: CompletionId,
        operation_id: OperationId,
    },
    Faulted,
}

/// Failure while preserving the two-phase completion ownership handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletionReclaimError {
    /// A method was called before its required preceding phase.
    InvalidPhase,
    /// The registry produced a completion generation with no exact binding.
    UnknownBinding(CompletionId),
    /// The operation no longer names the completion generation being reclaimed.
    BindingMismatch,
    /// The completion registry rejected the requested lifecycle step.
    Registry(CompletionRegistryError),
    /// The binding owner rejected exact final removal.
    Binding(OperationBindingError),
}

impl fmt::Display for CompletionReclaimError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPhase => formatter.write_str("completion reclaim phase is invalid"),
            Self::UnknownBinding(_) => {
                formatter.write_str("reclaim-ready completion has no operation binding")
            }
            Self::BindingMismatch => {
                formatter.write_str("completion reclaim binding generation changed")
            }
            Self::Registry(error) => {
                write!(formatter, "completion registry rejected reclaim: {error}")
            }
            Self::Binding(error) => {
                write!(formatter, "completion binding rejected reclaim: {error}")
            }
        }
    }
}

impl Error for CompletionReclaimError {}

impl From<CompletionRegistryError> for CompletionReclaimError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Registry(error)
    }
}

/// Result of one engine-side finish attempt after core accepted the input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletionReclaimOutcome {
    /// Observer state is briefly locked; retry only the finish phase.
    Retry,
    /// Registry capacity and the exact operation binding were reclaimed.
    Reclaimed {
        /// Core operation whose terminal ownership ended.
        operation_id: OperationId,
        /// Exact engine completion generation that was recycled.
        completion_id: CompletionId,
    },
    /// The exhausted registry slot was retired and its exact binding removed.
    Retired {
        /// Core operation whose terminal ownership ended.
        operation_id: OperationId,
        /// Exact engine completion generation that exhausted.
        completion_id: CompletionId,
    },
}

/// Linear owner preventing duplicate core reclamation input emission.
#[derive(Debug)]
pub(crate) struct CompletionReclaimer {
    phase: ReclaimPhase,
}

impl CompletionReclaimer {
    /// Creates an idle reclaimer with no in-flight registry identity.
    pub(crate) const fn new() -> Self {
        Self {
            phase: ReclaimPhase::Idle,
        }
    }

    /// Reports whether the next host turn must retry engine finishing only.
    pub(crate) const fn finish_pending(&self) -> bool {
        matches!(self.phase, ReclaimPhase::Finishing { .. })
    }

    /// Forgets an unrecoverable reclaim handshake after host termination.
    pub(crate) fn clear_terminal(&mut self) {
        self.phase = ReclaimPhase::Idle;
    }

    /// Obtains at most one reclaim identity and emits its core input once.
    pub(crate) fn next_input(
        &mut self,
        registry: &mut CompletionRegistry<ProducerCompletion>,
        bindings: &OperationBindings,
    ) -> Result<Option<ProducerInput>, CompletionReclaimError> {
        if self.phase != ReclaimPhase::Idle {
            return Err(CompletionReclaimError::InvalidPhase);
        }
        let Some(completion_id) = registry.next_reclaim()? else {
            return Ok(None);
        };
        let Some(operation_id) = bindings.operation(completion_id) else {
            self.phase = ReclaimPhase::Faulted;
            return Err(CompletionReclaimError::UnknownBinding(completion_id));
        };
        if bindings.completion(operation_id) != Some(completion_id) {
            self.phase = ReclaimPhase::Faulted;
            return Err(CompletionReclaimError::BindingMismatch);
        }
        self.phase = ReclaimPhase::AwaitingCore {
            completion_id,
            operation_id,
        };
        Ok(Some(ProducerInput::CompletionReclaimed { operation_id }))
    }

    /// Confirms successful `ProducerMachine::apply` and starts engine finish.
    pub(crate) fn confirm_core_applied(
        &mut self,
        registry: &mut CompletionRegistry<ProducerCompletion>,
        bindings: &mut OperationBindings,
    ) -> Result<CompletionReclaimOutcome, CompletionReclaimError> {
        let ReclaimPhase::AwaitingCore {
            completion_id,
            operation_id,
        } = self.phase
        else {
            return Err(CompletionReclaimError::InvalidPhase);
        };
        self.phase = ReclaimPhase::Finishing {
            completion_id,
            operation_id,
        };
        self.finish(registry, bindings)
    }

    /// Retries only registry recycling after a prior `Retry` outcome.
    pub(crate) fn retry_finish(
        &mut self,
        registry: &mut CompletionRegistry<ProducerCompletion>,
        bindings: &mut OperationBindings,
    ) -> Result<CompletionReclaimOutcome, CompletionReclaimError> {
        if !matches!(self.phase, ReclaimPhase::Finishing { .. }) {
            return Err(CompletionReclaimError::InvalidPhase);
        }
        self.finish(registry, bindings)
    }

    fn finish(
        &mut self,
        registry: &mut CompletionRegistry<ProducerCompletion>,
        bindings: &mut OperationBindings,
    ) -> Result<CompletionReclaimOutcome, CompletionReclaimError> {
        let ReclaimPhase::Finishing {
            completion_id,
            operation_id,
        } = self.phase
        else {
            return Err(CompletionReclaimError::InvalidPhase);
        };
        if bindings.completion(operation_id) != Some(completion_id) {
            self.phase = ReclaimPhase::Faulted;
            return Err(CompletionReclaimError::BindingMismatch);
        }
        match registry.finish_reclaim(completion_id) {
            Ok(ReclaimStatus::Retry) => Ok(CompletionReclaimOutcome::Retry),
            Ok(ReclaimStatus::Reclaimed) => {
                self.finish_binding(bindings, operation_id, completion_id)?;
                Ok(CompletionReclaimOutcome::Reclaimed {
                    operation_id,
                    completion_id,
                })
            }
            Err(CompletionRegistryError::GenerationExhausted) => {
                self.finish_binding(bindings, operation_id, completion_id)?;
                Ok(CompletionReclaimOutcome::Retired {
                    operation_id,
                    completion_id,
                })
            }
            Err(error) => {
                self.phase = ReclaimPhase::Faulted;
                Err(CompletionReclaimError::Registry(error))
            }
        }
    }

    fn finish_binding(
        &mut self,
        bindings: &mut OperationBindings,
        operation_id: OperationId,
        completion_id: CompletionId,
    ) -> Result<(), CompletionReclaimError> {
        if let Err(error) = bindings.remove_exact(operation_id, completion_id) {
            self.phase = ReclaimPhase::Faulted;
            return Err(CompletionReclaimError::Binding(error));
        }
        self.phase = ReclaimPhase::Idle;
        Ok(())
    }
}
