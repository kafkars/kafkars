//! Fixed-capacity association of operations with engine execution ownership.

use std::{error::Error, fmt};

use kafka_client_core::OperationId;

use crate::{clock::OperationDeadline, completion::CompletionId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OperationBinding {
    operation_id: OperationId,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    waiting_origin: bool,
    waiting_terminal: bool,
}

/// Failure to mutate producer operation execution ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OperationBindingError {
    /// Every preallocated binding entry is occupied.
    Full,
    /// The operation already owns a completion binding.
    DuplicateOperation,
    /// The completion generation is already bound to another operation.
    DuplicateCompletion,
    /// Removal named an operation with no live binding.
    UnknownOperation,
    /// Removal named the right operation but the wrong completion generation.
    CompletionMismatch,
    /// The operation was not accepted through bounded waiting ownership.
    NotWaitingOperation,
}

impl fmt::Display for OperationBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Full => "producer completion bindings are full",
            Self::DuplicateOperation => "producer operation already owns a completion",
            Self::DuplicateCompletion => "engine completion already belongs to an operation",
            Self::UnknownOperation => "producer operation owns no completion binding",
            Self::CompletionMismatch => "producer operation owns a different completion generation",
            Self::NotWaitingOperation => "producer operation did not originate in waiting",
        })
    }
}

impl Error for OperationBindingError {}

/// Linear fixed-capacity owner of operation, completion, and deadline facts.
#[derive(Debug)]
pub(crate) struct OperationBindings {
    max_entries: usize,
    entries: Vec<OperationBinding>,
}

impl OperationBindings {
    /// Preallocates the complete association capacity.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            max_entries: capacity,
            entries: Vec::with_capacity(capacity),
        }
    }

    /// Associates one core operation with one exact completion generation.
    pub(crate) fn bind(
        &mut self,
        operation_id: OperationId,
        completion_id: CompletionId,
        deadline: OperationDeadline,
    ) -> Result<(), OperationBindingError> {
        self.bind_with_origin(operation_id, completion_id, deadline, false)
    }

    /// Associates an operation accepted through bounded waiting ownership.
    pub(crate) fn bind_waiting(
        &mut self,
        operation_id: OperationId,
        completion_id: CompletionId,
        deadline: OperationDeadline,
    ) -> Result<(), OperationBindingError> {
        self.bind_with_origin(operation_id, completion_id, deadline, true)
    }

    fn bind_with_origin(
        &mut self,
        operation_id: OperationId,
        completion_id: CompletionId,
        deadline: OperationDeadline,
        waiting_origin: bool,
    ) -> Result<(), OperationBindingError> {
        let index = match self.operation_index(operation_id) {
            Ok(_) => return Err(OperationBindingError::DuplicateOperation),
            Err(index) => index,
        };
        if self
            .entries
            .iter()
            .any(|binding| binding.completion_id == completion_id)
        {
            return Err(OperationBindingError::DuplicateCompletion);
        }
        if self.entries.len() >= self.max_entries {
            return Err(OperationBindingError::Full);
        }
        self.entries.insert(
            index,
            OperationBinding {
                operation_id,
                completion_id,
                deadline,
                waiting_origin,
                waiting_terminal: false,
            },
        );
        Ok(())
    }

    /// Marks one waiting-origin association terminal until exact reclaim.
    pub(crate) fn mark_waiting_terminal(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), OperationBindingError> {
        let index = self
            .operation_index(operation_id)
            .map_err(|_| OperationBindingError::UnknownOperation)?;
        if !self.entries[index].waiting_origin {
            return Err(OperationBindingError::NotWaitingOperation);
        }
        self.entries[index].waiting_terminal = true;
        Ok(())
    }

    /// Returns waiting-origin terminal associations retained until reclaim.
    pub(crate) fn waiting_terminal_len(&self) -> usize {
        self.entries
            .iter()
            .filter(|binding| binding.waiting_terminal)
            .count()
    }

    /// Returns the exact completion generation bound to an operation.
    pub(crate) fn completion(&self, operation_id: OperationId) -> Option<CompletionId> {
        self.operation_index(operation_id)
            .ok()
            .map(|index| self.entries[index].completion_id)
    }

    /// Returns the operation bound to an exact completion generation.
    pub(crate) fn operation(&self, completion_id: CompletionId) -> Option<OperationId> {
        self.entries
            .iter()
            .find(|binding| binding.completion_id == completion_id)
            .map(|binding| binding.operation_id)
    }

    /// Returns the original paired deadline bound at public admission.
    pub(crate) fn deadline(&self, operation_id: OperationId) -> Option<OperationDeadline> {
        self.operation_index(operation_id)
            .ok()
            .map(|index| self.entries[index].deadline)
    }

    /// Removes an operation association and returns its completion generation.
    pub(crate) fn remove(
        &mut self,
        operation_id: OperationId,
    ) -> Result<CompletionId, OperationBindingError> {
        let index = self
            .operation_index(operation_id)
            .map_err(|_| OperationBindingError::UnknownOperation)?;
        Ok(self.entries.remove(index).completion_id)
    }

    /// Removes only the exact operation and completion generation association.
    pub(crate) fn remove_exact(
        &mut self,
        operation_id: OperationId,
        completion_id: CompletionId,
    ) -> Result<(), OperationBindingError> {
        let index = self
            .operation_index(operation_id)
            .map_err(|_| OperationBindingError::UnknownOperation)?;
        if self.entries[index].completion_id != completion_id {
            return Err(OperationBindingError::CompletionMismatch);
        }
        self.entries.remove(index);
        Ok(())
    }

    /// Drops terminal-only associations after observer publication.
    pub(crate) fn clear_terminal(&mut self) {
        self.entries.clear();
    }

    /// Returns the number of live operation associations.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    fn operation_index(&self, operation_id: OperationId) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&operation_id, |binding| binding.operation_id)
    }

    #[cfg(test)]
    pub(crate) fn allocation_capacity(&self) -> usize {
        self.entries.capacity()
    }
}
