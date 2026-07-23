//! Exact binding, generation, duplicate, and capacity validation for terminals.

use kafka_client_core::{OperationId, ProducerCompletion, ProducerEffect};

use crate::completion::CompletionId;

use super::super::{
    ProducerHost, ProducerHostInvariantError, binding::OperationBindingError,
    terminal_backlog::RetainedTerminal,
};

impl ProducerHost {
    /// Preserves only exact terminal decisions after interpretation has stopped.
    ///
    /// Mechanism effects and generated facts remain owned by their real stores
    /// and are drained during poisoned recovery.
    pub(in crate::producer) fn retain_terminal_tail(&mut self, effects: &[ProducerEffect]) {
        for effect in effects {
            let ProducerEffect::Complete {
                operation_id,
                completion,
            } = *effect
            else {
                continue;
            };
            let Ok(terminal) = self.validate_terminal(operation_id, completion) else {
                // Its reserved registry slot receives the conservative fallback.
                continue;
            };
            // Failed FIFO transfer leaves the same registry slot authoritative.
            let _retained = self.retain_validated_terminal(terminal);
        }
    }

    pub(super) fn validate_terminal(
        &mut self,
        operation_id: OperationId,
        completion: ProducerCompletion,
    ) -> Result<RetainedTerminal, ProducerHostInvariantError> {
        let Some(completion_id) = self.bindings.completion(operation_id) else {
            let error =
                ProducerHostInvariantError::Binding(OperationBindingError::UnknownOperation);
            return Err(self.poison(error));
        };
        if let Err(error) = self.revalidate_terminal(operation_id, completion_id) {
            return Err(self.poison(error));
        }
        Ok(RetainedTerminal::new(
            operation_id,
            completion_id,
            completion,
        ))
    }

    pub(super) fn revalidate_terminal(
        &self,
        operation_id: OperationId,
        completion_id: CompletionId,
    ) -> Result<(), ProducerHostInvariantError> {
        match self.bindings.completion(operation_id) {
            None => {
                return Err(ProducerHostInvariantError::Binding(
                    OperationBindingError::UnknownOperation,
                ));
            }
            Some(current) if current != completion_id => {
                return Err(ProducerHostInvariantError::Binding(
                    OperationBindingError::CompletionMismatch,
                ));
            }
            Some(_) => {}
        }
        self.completions
            .validate_reserved(completion_id)
            .map_err(ProducerHostInvariantError::Completion)
    }

    pub(super) fn retain_validated_terminal(
        &mut self,
        terminal: RetainedTerminal,
    ) -> Result<(), ProducerHostInvariantError> {
        let duplicate = if self
            .terminal_backlog
            .contains_operation(terminal.operation_id())
        {
            Some(OperationBindingError::DuplicateOperation)
        } else if self
            .terminal_backlog
            .contains_completion(terminal.completion_id())
        {
            Some(OperationBindingError::DuplicateCompletion)
        } else {
            None
        };
        if let Some(error) = duplicate {
            let invariant = ProducerHostInvariantError::Binding(error);
            return Err(self.poison(invariant));
        }
        let occupied = self
            .terminal_backlog
            .len()
            .saturating_add(self.completions.published_or_reclaiming_len());
        if occupied >= self.effect_capacity {
            let error = ProducerHostInvariantError::TerminalBacklogCapacity;
            return Err(self.poison(error));
        }
        self.terminal_backlog.push(terminal);
        Ok(())
    }
}
