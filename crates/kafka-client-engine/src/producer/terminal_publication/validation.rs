//! Exact binding, generation, duplicate, and capacity validation for terminals.

use kafka_client_core::{OperationId, ProducerCompletion};

use crate::completion::CompletionId;

use super::super::{
    ProducerHost, ProducerHostInvariantError,
    binding::CompletionBindingError,
    terminal_backlog::{RejectedTerminal, RetainedTerminal},
};

impl ProducerHost {
    /// Transfers a tail terminal only when every normal FIFO proof still holds.
    ///
    /// Failure leaves the original `ProducerEffect::Complete` in the committed
    /// tail vector owned by the caller.
    pub(in crate::producer) fn retain_terminal_tail(
        &mut self,
        operation_id: OperationId,
        completion: ProducerCompletion,
    ) -> Result<(), ProducerHostInvariantError> {
        let Some(completion_id) = self.bindings.completion(operation_id) else {
            let error =
                ProducerHostInvariantError::Binding(CompletionBindingError::UnknownOperation);
            return Err(self.poison(error));
        };
        if let Err(error) = self.revalidate_terminal(operation_id, completion_id) {
            return Err(self.poison(error));
        }
        let terminal = RetainedTerminal::new(operation_id, completion_id, completion);
        if self
            .terminal_backlog
            .contains_operation(terminal.operation_id())
        {
            let error =
                ProducerHostInvariantError::Binding(CompletionBindingError::DuplicateOperation);
            return Err(self.poison(error));
        }
        if self
            .terminal_backlog
            .contains_completion(terminal.completion_id())
        {
            let error =
                ProducerHostInvariantError::Binding(CompletionBindingError::DuplicateCompletion);
            return Err(self.poison(error));
        }
        let occupied = self
            .terminal_backlog
            .len()
            .saturating_add(self.completions.published_or_reclaiming_len());
        if occupied >= self.effect_capacity {
            return Err(self.poison(ProducerHostInvariantError::TerminalBacklogCapacity));
        }
        self.terminal_backlog.push(terminal);
        Ok(())
    }

    pub(super) fn validate_terminal(
        &mut self,
        operation_id: OperationId,
        completion: ProducerCompletion,
    ) -> Result<RetainedTerminal, ProducerHostInvariantError> {
        let Some(completion_id) = self.bindings.completion(operation_id) else {
            let error =
                ProducerHostInvariantError::Binding(CompletionBindingError::UnknownOperation);
            return Err(self.quarantine_rejected(RejectedTerminal::new(
                operation_id,
                None,
                completion,
                error,
            )));
        };
        if let Err(error) = self.revalidate_terminal(operation_id, completion_id) {
            return Err(self.quarantine_rejected(RejectedTerminal::new(
                operation_id,
                Some(completion_id),
                completion,
                error,
            )));
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
                    CompletionBindingError::UnknownOperation,
                ));
            }
            Some(current) if current != completion_id => {
                return Err(ProducerHostInvariantError::Binding(
                    CompletionBindingError::CompletionMismatch,
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
            Some(CompletionBindingError::DuplicateOperation)
        } else if self
            .terminal_backlog
            .contains_completion(terminal.completion_id())
        {
            Some(CompletionBindingError::DuplicateCompletion)
        } else {
            None
        };
        if let Some(error) = duplicate {
            let invariant = ProducerHostInvariantError::Binding(error);
            return Err(self.quarantine_rejected(RejectedTerminal::new(
                terminal.operation_id(),
                Some(terminal.completion_id()),
                terminal.completion(),
                invariant,
            )));
        }
        let occupied = self
            .terminal_backlog
            .len()
            .saturating_add(self.completions.published_or_reclaiming_len());
        if occupied >= self.effect_capacity {
            let error = ProducerHostInvariantError::TerminalBacklogCapacity;
            return Err(self.quarantine_rejected(RejectedTerminal::new(
                terminal.operation_id(),
                Some(terminal.completion_id()),
                terminal.completion(),
                error,
            )));
        }
        self.terminal_backlog.push(terminal);
        Ok(())
    }

    pub(super) fn quarantine_rejected(
        &mut self,
        terminal: RejectedTerminal,
    ) -> ProducerHostInvariantError {
        let reason = terminal.reason();
        if let Err(occupied) = self.terminal_poison.retain(terminal) {
            let Some(vacancy) = self.terminal_refusals.terminal_vacancy() else {
                return self.poison(ProducerHostInvariantError::TerminalBacklogCorrupt);
            };
            if let Err(refused) = self.terminal_quarantine.retain_terminal(occupied) {
                vacancy.retain(refused);
                return self.poison(ProducerHostInvariantError::TerminalQuarantineCapacity);
            }
        }
        self.poison(reason)
    }
}
