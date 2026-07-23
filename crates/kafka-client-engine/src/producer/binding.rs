//! Fixed-capacity association of core operations with engine completion slots.

use std::{error::Error, fmt};

use kafka_client_core::OperationId;

use crate::completion::CompletionId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CompletionBinding {
    operation_id: OperationId,
    completion_id: CompletionId,
}

/// Failure to mutate producer operation-to-completion ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletionBindingError {
    /// Every preallocated binding entry is occupied.
    Full,
    /// The operation already owns a completion binding.
    DuplicateOperation,
    /// The completion generation is already bound to another operation.
    DuplicateCompletion,
    /// Removal named an operation with no live binding.
    UnknownOperation,
}

impl fmt::Display for CompletionBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Full => "producer completion bindings are full",
            Self::DuplicateOperation => "producer operation already owns a completion",
            Self::DuplicateCompletion => "engine completion already belongs to an operation",
            Self::UnknownOperation => "producer operation owns no completion binding",
        })
    }
}

impl Error for CompletionBindingError {}

/// Linear fixed-capacity owner of operation and completion associations.
#[derive(Debug)]
pub(crate) struct CompletionBindings {
    max_entries: usize,
    entries: Vec<CompletionBinding>,
}

impl CompletionBindings {
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
    ) -> Result<(), CompletionBindingError> {
        let index = match self.operation_index(operation_id) {
            Ok(_) => return Err(CompletionBindingError::DuplicateOperation),
            Err(index) => index,
        };
        if self
            .entries
            .iter()
            .any(|binding| binding.completion_id == completion_id)
        {
            return Err(CompletionBindingError::DuplicateCompletion);
        }
        if self.entries.len() >= self.max_entries {
            return Err(CompletionBindingError::Full);
        }
        self.entries.insert(
            index,
            CompletionBinding {
                operation_id,
                completion_id,
            },
        );
        Ok(())
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

    /// Removes an operation association and returns its completion generation.
    pub(crate) fn remove(
        &mut self,
        operation_id: OperationId,
    ) -> Result<CompletionId, CompletionBindingError> {
        let index = self
            .operation_index(operation_id)
            .map_err(|_| CompletionBindingError::UnknownOperation)?;
        Ok(self.entries.remove(index).completion_id)
    }

    fn operation_index(&self, operation_id: OperationId) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&operation_id, |binding| binding.operation_id)
    }
}
