//! Ordered producer-terminal validation, publication, and retry.

mod validation;
#[cfg(test)]
mod validation_test;

use kafka_client_core::{FlushId, OperationId, ProducerCompletion};

use crate::completion::{CompletionId, CompletionRegistryError};

use super::{
    ProducerHost, ProducerHostInvariantError, terminal::ProducerTerminal,
    terminal_backlog::RetainedTerminal,
};

impl ProducerHost {
    pub(super) fn publish_or_retain_record_terminal(
        &mut self,
        operation_id: OperationId,
        completion: ProducerCompletion,
    ) -> Result<(), ProducerHostInvariantError> {
        let terminal = self.validate_record_terminal(operation_id, completion)?;
        self.publish_or_retain_validated(terminal)
    }

    pub(super) fn publish_or_retain_flush_terminal(
        &mut self,
        flush_id: FlushId,
    ) -> Result<(), ProducerHostInvariantError> {
        let terminal = self.validate_flush_terminal(flush_id)?;
        self.publish_or_retain_validated(terminal)
    }

    fn publish_or_retain_validated(
        &mut self,
        terminal: RetainedTerminal,
    ) -> Result<(), ProducerHostInvariantError> {
        if !self.terminal_backlog.is_empty() {
            return self.retain_validated_terminal(terminal);
        }
        let completion_id = terminal.completion_id();
        match self.publish_terminal(completion_id, terminal.terminal()) {
            Ok(()) => Ok(()),
            Err((error, _value)) => {
                self.retain_validated_terminal(terminal)?;
                if error == CompletionRegistryError::NotificationBackpressure {
                    Ok(())
                } else {
                    Err(self.poison(ProducerHostInvariantError::Completion(error)))
                }
            }
        }
    }

    /// Retries valid producer terminals from the front only.
    pub(crate) fn retry_terminal_backlog(
        &mut self,
        limit: usize,
    ) -> Result<usize, ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        self.retry_terminal_backlog_inner(limit, false)
    }

    /// Recovery retries valid terminals and drops invalid copies.
    pub(super) fn retry_terminal_backlog_for_recovery(
        &mut self,
        limit: usize,
    ) -> Result<usize, ProducerHostInvariantError> {
        self.retry_terminal_backlog_inner(limit, true)
    }

    fn retry_terminal_backlog_inner(
        &mut self,
        limit: usize,
        recovery: bool,
    ) -> Result<usize, ProducerHostInvariantError> {
        let mut published = 0;
        let mut examined = 0;
        let mut first_corruption = None;
        while examined < limit {
            let Some(terminal) = self.terminal_backlog.front() else {
                break;
            };
            let owner = terminal.owner();
            let completion_id = terminal.completion_id();
            let value = terminal.terminal();
            if let Err(error) = self.revalidate_terminal(owner, completion_id) {
                let removed = self.terminal_backlog.pop_published();
                debug_assert!(removed.is_some());
                let poisoned = self.poison(error);
                if !recovery {
                    return Err(poisoned);
                }
                first_corruption.get_or_insert(poisoned);
                examined += 1;
                continue;
            }
            match self.publish_terminal(completion_id, value) {
                Ok(()) => {
                    let removed = self.terminal_backlog.pop_published();
                    debug_assert!(removed.is_some());
                    published += 1;
                    examined += 1;
                }
                Err((CompletionRegistryError::NotificationBackpressure, _)) => break,
                Err((error, _)) => {
                    return Err(self.poison(ProducerHostInvariantError::Completion(error)));
                }
            }
        }
        first_corruption.map_or(Ok(published), Err)
    }

    fn publish_terminal(
        &mut self,
        completion_id: CompletionId,
        terminal: ProducerTerminal,
    ) -> Result<(), (CompletionRegistryError, ProducerTerminal)> {
        #[cfg(test)]
        {
            self.terminal_publish_attempts = self.terminal_publish_attempts.saturating_add(1);
            if let Some(error) = self.terminal_publish_faults.pop_front() {
                return Err((error, terminal));
            }
        }
        self.completions.publish(completion_id, terminal)
    }

    #[cfg(test)]
    pub(super) fn inject_terminal_publish_fault(&mut self, error: CompletionRegistryError) {
        self.terminal_publish_faults.push_back(error);
    }

    #[cfg(test)]
    pub(super) const fn terminal_publish_attempts(&self) -> usize {
        self.terminal_publish_attempts
    }

    #[cfg(test)]
    pub(super) fn terminal_front(&self) -> Option<&RetainedTerminal> {
        self.terminal_backlog.front()
    }

    #[cfg(test)]
    pub(super) fn terminal_back(&self) -> Option<&RetainedTerminal> {
        self.terminal_backlog.back()
    }
}
