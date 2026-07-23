//! Ordered record-terminal validation, publication, retry, and quarantine.

mod validation;
#[cfg(test)]
mod validation_test;

use kafka_client_core::{OperationId, ProducerCompletion};

use crate::completion::{CompletionId, CompletionRegistryError};

use super::{
    ProducerHost, ProducerHostInvariantError,
    terminal_backlog::{RejectedTerminal, RetainedTerminal},
};

impl ProducerHost {
    pub(super) fn publish_or_retain_terminal(
        &mut self,
        operation_id: OperationId,
        completion: ProducerCompletion,
    ) -> Result<(), ProducerHostInvariantError> {
        let terminal = self.validate_terminal(operation_id, completion)?;
        if !self.terminal_backlog.is_empty() {
            return self.retain_validated_terminal(terminal);
        }
        let completion_id = terminal.completion_id();
        match self.publish_terminal(completion_id, completion) {
            Ok(()) => Ok(()),
            Err((error, retained)) => {
                self.retain_validated_terminal(RetainedTerminal::new(
                    operation_id,
                    completion_id,
                    retained,
                ))?;
                if error == CompletionRegistryError::NotificationBackpressure {
                    Ok(())
                } else {
                    Err(self.poison(ProducerHostInvariantError::Completion(error)))
                }
            }
        }
    }

    /// Retries valid record terminals from the front only.
    pub(crate) fn retry_terminal_backlog(
        &mut self,
        limit: usize,
    ) -> Result<usize, ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        self.retry_terminal_backlog_inner(limit, false)
    }

    /// Recovery retries valid terminals and quarantines corrupt entries.
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
            let operation_id = terminal.operation_id();
            let completion_id = terminal.completion_id();
            let completion = terminal.completion();
            if let Err(error) = self.revalidate_terminal(operation_id, completion_id) {
                let Some(removed) = self.terminal_backlog.pop_rejected() else {
                    return Err(self.poison(ProducerHostInvariantError::TerminalBacklogCorrupt));
                };
                let poisoned = self.quarantine_rejected(RejectedTerminal::new(
                    removed.operation_id(),
                    Some(removed.completion_id()),
                    removed.completion(),
                    error,
                ));
                if !recovery {
                    return Err(poisoned);
                }
                first_corruption.get_or_insert(poisoned);
                examined += 1;
                continue;
            }
            match self.publish_terminal(completion_id, completion) {
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
        completion: ProducerCompletion,
    ) -> Result<(), (CompletionRegistryError, ProducerCompletion)> {
        #[cfg(test)]
        {
            self.terminal_publish_attempts = self.terminal_publish_attempts.saturating_add(1);
            if let Some(error) = self.terminal_publish_faults.pop_front() {
                return Err((error, completion));
            }
        }
        self.completions.publish(completion_id, completion)
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

    #[cfg(test)]
    pub(super) const fn terminal_poison(&self) -> Option<&RejectedTerminal> {
        self.terminal_poison.evidence()
    }
}
