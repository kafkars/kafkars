//! Two-phase core and engine ownership of terminal completion reclamation.

mod error;
#[cfg(test)]
mod error_test;
mod outcome;
pub(crate) use error::CompletionReclaimError;
pub(crate) use outcome::CompletionReclaimOutcome;

use kafka_client_core::ProducerInput;

use crate::completion::{CompletionId, CompletionRegistry, CompletionRegistryError, ReclaimStatus};

use super::{
    binding::OperationBindings, flush::FlushBindings, terminal::ProducerTerminal,
    terminal_backlog::ProducerTerminalOwner,
};
use outcome::{owner_completion, reclaim_input};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReclaimPhase {
    Idle,
    AwaitingCore {
        completion_id: CompletionId,
        owner: ProducerTerminalOwner,
    },
    Finishing {
        completion_id: CompletionId,
        owner: ProducerTerminalOwner,
    },
    Faulted,
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
        registry: &mut CompletionRegistry<ProducerTerminal>,
        bindings: &OperationBindings,
        flush_bindings: &FlushBindings,
    ) -> Result<Option<ProducerInput>, CompletionReclaimError> {
        if self.phase != ReclaimPhase::Idle {
            return Err(CompletionReclaimError::InvalidPhase);
        }
        let Some(completion_id) = registry.next_reclaim()? else {
            return Ok(None);
        };
        let operation = bindings
            .operation(completion_id)
            .map(ProducerTerminalOwner::Record);
        let flush = flush_bindings
            .flush(completion_id)
            .map(ProducerTerminalOwner::Flush);
        let owner = match (operation, flush) {
            (Some(owner), None) | (None, Some(owner)) => owner,
            (None, None) => {
                self.phase = ReclaimPhase::Faulted;
                return Err(CompletionReclaimError::UnknownBinding(completion_id));
            }
            (Some(_), Some(_)) => {
                self.phase = ReclaimPhase::Faulted;
                return Err(CompletionReclaimError::AmbiguousBinding(completion_id));
            }
        };
        if owner_completion(owner, bindings, flush_bindings) != Some(completion_id) {
            self.phase = ReclaimPhase::Faulted;
            return Err(CompletionReclaimError::BindingMismatch);
        }
        self.phase = ReclaimPhase::AwaitingCore {
            completion_id,
            owner,
        };
        Ok(Some(reclaim_input(owner)))
    }

    /// Confirms successful `ProducerMachine::apply` and starts engine finish.
    pub(crate) fn confirm_core_applied(
        &mut self,
        registry: &mut CompletionRegistry<ProducerTerminal>,
        bindings: &mut OperationBindings,
        flush_bindings: &mut FlushBindings,
    ) -> Result<CompletionReclaimOutcome, CompletionReclaimError> {
        let ReclaimPhase::AwaitingCore {
            completion_id,
            owner,
        } = self.phase
        else {
            return Err(CompletionReclaimError::InvalidPhase);
        };
        self.phase = ReclaimPhase::Finishing {
            completion_id,
            owner,
        };
        self.finish(registry, bindings, flush_bindings)
    }

    /// Retries only registry recycling after a prior `Retry` outcome.
    pub(crate) fn retry_finish(
        &mut self,
        registry: &mut CompletionRegistry<ProducerTerminal>,
        bindings: &mut OperationBindings,
        flush_bindings: &mut FlushBindings,
    ) -> Result<CompletionReclaimOutcome, CompletionReclaimError> {
        if !matches!(self.phase, ReclaimPhase::Finishing { .. }) {
            return Err(CompletionReclaimError::InvalidPhase);
        }
        self.finish(registry, bindings, flush_bindings)
    }

    fn finish(
        &mut self,
        registry: &mut CompletionRegistry<ProducerTerminal>,
        bindings: &mut OperationBindings,
        flush_bindings: &mut FlushBindings,
    ) -> Result<CompletionReclaimOutcome, CompletionReclaimError> {
        let ReclaimPhase::Finishing {
            completion_id,
            owner,
        } = self.phase
        else {
            return Err(CompletionReclaimError::InvalidPhase);
        };
        if owner_completion(owner, bindings, flush_bindings) != Some(completion_id) {
            self.phase = ReclaimPhase::Faulted;
            return Err(CompletionReclaimError::BindingMismatch);
        }
        match registry.finish_reclaim(completion_id) {
            Ok(ReclaimStatus::Retry) => Ok(CompletionReclaimOutcome::Retry),
            Ok(ReclaimStatus::Reclaimed) => {
                self.finish_binding(bindings, flush_bindings, owner, completion_id)?;
                Ok(CompletionReclaimOutcome::Reclaimed {
                    owner,
                    completion_id,
                })
            }
            Err(CompletionRegistryError::GenerationExhausted) => {
                self.finish_binding(bindings, flush_bindings, owner, completion_id)?;
                Ok(CompletionReclaimOutcome::Retired {
                    owner,
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
        flush_bindings: &mut FlushBindings,
        owner: ProducerTerminalOwner,
        completion_id: CompletionId,
    ) -> Result<(), CompletionReclaimError> {
        let result = match owner {
            ProducerTerminalOwner::Record(operation_id) => bindings
                .remove_exact(operation_id, completion_id)
                .map_err(CompletionReclaimError::Binding),
            ProducerTerminalOwner::Flush(flush_id) => flush_bindings
                .remove_exact(flush_id, completion_id)
                .map_err(CompletionReclaimError::FlushBinding),
        };
        if let Err(error) = result {
            self.phase = ReclaimPhase::Faulted;
            return Err(error);
        }
        self.phase = ReclaimPhase::Idle;
        Ok(())
    }
}
