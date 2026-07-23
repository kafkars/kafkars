//! Exact producer owner, generation, duplicate, and capacity validation.

use kafka_client_core::{FlushId, OperationId, ProducerCompletion, ProducerEffect};

use crate::completion::CompletionId;

use super::super::{
    ProducerHost, ProducerHostInvariantError,
    binding::OperationBindingError,
    flush::FlushBindingError,
    terminal_backlog::{ProducerTerminalOwner, RetainedTerminal},
};

impl ProducerHost {
    /// Preserves exact terminal decisions after interpretation has stopped.
    pub(in crate::producer) fn retain_terminal_tail(&mut self, effects: &[ProducerEffect]) {
        for effect in effects {
            let terminal = match *effect {
                ProducerEffect::Complete {
                    operation_id,
                    completion,
                } => self.validate_record_terminal(operation_id, completion),
                ProducerEffect::CompleteFlush { flush_id } => {
                    self.validate_flush_terminal(flush_id)
                }
                _ => continue,
            };
            let Ok(terminal) = terminal else {
                // Its reserved registry slot receives the conservative fallback.
                continue;
            };
            // Failed FIFO transfer leaves the same registry slot authoritative.
            let _retained = self.retain_validated_terminal(terminal);
        }
    }

    pub(super) fn validate_record_terminal(
        &mut self,
        operation_id: OperationId,
        completion: ProducerCompletion,
    ) -> Result<RetainedTerminal, ProducerHostInvariantError> {
        let owner = ProducerTerminalOwner::Record(operation_id);
        let completion_id = self.completion_for(owner)?;
        self.revalidate_terminal(owner, completion_id)
            .map_err(|error| self.poison(error))?;
        Ok(RetainedTerminal::record(
            operation_id,
            completion_id,
            completion,
        ))
    }

    pub(super) fn validate_flush_terminal(
        &mut self,
        flush_id: FlushId,
    ) -> Result<RetainedTerminal, ProducerHostInvariantError> {
        let owner = ProducerTerminalOwner::Flush(flush_id);
        let completion_id = self.completion_for(owner)?;
        self.revalidate_terminal(owner, completion_id)
            .map_err(|error| self.poison(error))?;
        Ok(RetainedTerminal::flush(flush_id, completion_id))
    }

    pub(super) fn revalidate_terminal(
        &self,
        owner: ProducerTerminalOwner,
        completion_id: CompletionId,
    ) -> Result<(), ProducerHostInvariantError> {
        let current = self.completion_for_read(owner)?;
        if current != completion_id {
            return Err(owner_mismatch(owner));
        }
        self.completions
            .validate_reserved(completion_id)
            .map_err(ProducerHostInvariantError::Completion)
    }

    pub(super) fn retain_validated_terminal(
        &mut self,
        terminal: RetainedTerminal,
    ) -> Result<(), ProducerHostInvariantError> {
        if self.terminal_backlog.contains_owner(terminal.owner()) {
            let error = duplicate_owner(terminal.owner());
            return Err(self.poison(error));
        }
        if self
            .terminal_backlog
            .contains_completion(terminal.completion_id())
        {
            let error = duplicate_completion(terminal.owner());
            return Err(self.poison(error));
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

    fn completion_for(
        &mut self,
        owner: ProducerTerminalOwner,
    ) -> Result<CompletionId, ProducerHostInvariantError> {
        self.completion_for_read(owner)
            .map_err(|error| self.poison(error))
    }

    fn completion_for_read(
        &self,
        owner: ProducerTerminalOwner,
    ) -> Result<CompletionId, ProducerHostInvariantError> {
        match owner {
            ProducerTerminalOwner::Record(operation_id) => self
                .bindings
                .completion(operation_id)
                .ok_or(ProducerHostInvariantError::Binding(
                    OperationBindingError::UnknownOperation,
                )),
            ProducerTerminalOwner::Flush(flush_id) => {
                self.flush_bindings.completion(flush_id).ok_or(
                    ProducerHostInvariantError::FlushBinding(FlushBindingError::UnknownFlush),
                )
            }
        }
    }
}

const fn owner_mismatch(owner: ProducerTerminalOwner) -> ProducerHostInvariantError {
    match owner {
        ProducerTerminalOwner::Record(_) => {
            ProducerHostInvariantError::Binding(OperationBindingError::CompletionMismatch)
        }
        ProducerTerminalOwner::Flush(_) => {
            ProducerHostInvariantError::FlushBinding(FlushBindingError::CompletionMismatch)
        }
    }
}

const fn duplicate_owner(owner: ProducerTerminalOwner) -> ProducerHostInvariantError {
    match owner {
        ProducerTerminalOwner::Record(_) => {
            ProducerHostInvariantError::Binding(OperationBindingError::DuplicateOperation)
        }
        ProducerTerminalOwner::Flush(_) => {
            ProducerHostInvariantError::FlushBinding(FlushBindingError::DuplicateFlush)
        }
    }
}

const fn duplicate_completion(owner: ProducerTerminalOwner) -> ProducerHostInvariantError {
    match owner {
        ProducerTerminalOwner::Record(_) => {
            ProducerHostInvariantError::Binding(OperationBindingError::DuplicateCompletion)
        }
        ProducerTerminalOwner::Flush(_) => {
            ProducerHostInvariantError::FlushBinding(FlushBindingError::DuplicateCompletion)
        }
    }
}
