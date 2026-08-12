//! Fixed-capacity association of operations with engine execution ownership.

use std::{error::Error, fmt};

use kafka_client_core::OperationId;

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    id_hash::{IdMap, id_map},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OperationBinding {
    completion_id: CompletionId,
    deadline: OperationDeadline,
    waiting_origin: bool,
    waiting_terminal: bool,
}

/// Failure to mutate producer operation execution ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OperationBindingError {
    /// Every bounded binding entry is occupied.
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
    waiting_terminal: usize,
    by_operation: IdMap<OperationId, OperationBinding>,
    by_completion: IdMap<CompletionId, OperationId>,
}

impl OperationBindings {
    /// Declares the complete association capacity without touching unused slots.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            max_entries: capacity,
            waiting_terminal: 0,
            by_operation: id_map(),
            by_completion: id_map(),
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
        if self.by_operation.contains_key(&operation_id) {
            return Err(OperationBindingError::DuplicateOperation);
        }
        if self.by_completion.contains_key(&completion_id) {
            return Err(OperationBindingError::DuplicateCompletion);
        }
        if self.by_operation.len() >= self.max_entries {
            return Err(OperationBindingError::Full);
        }
        let replaced_operation = self.by_operation.insert(
            operation_id,
            OperationBinding {
                completion_id,
                deadline,
                waiting_origin,
                waiting_terminal: false,
            },
        );
        let replaced = self.by_completion.insert(completion_id, operation_id);
        debug_assert!(replaced_operation.is_none());
        debug_assert!(replaced.is_none());
        Ok(())
    }

    /// Marks one waiting-origin association terminal until exact reclaim.
    pub(crate) fn mark_waiting_terminal(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), OperationBindingError> {
        let binding = self
            .by_operation
            .get_mut(&operation_id)
            .ok_or(OperationBindingError::UnknownOperation)?;
        if !binding.waiting_origin {
            return Err(OperationBindingError::NotWaitingOperation);
        }
        if !binding.waiting_terminal {
            binding.waiting_terminal = true;
            self.waiting_terminal += 1;
        }
        Ok(())
    }

    /// Returns waiting-origin terminal associations retained until reclaim.
    pub(crate) const fn waiting_terminal_len(&self) -> usize {
        self.waiting_terminal
    }

    /// Returns the exact completion generation bound to an operation.
    pub(crate) fn completion(&self, operation_id: OperationId) -> Option<CompletionId> {
        self.by_operation
            .get(&operation_id)
            .map(|binding| binding.completion_id)
    }

    /// Returns the operation bound to an exact completion generation.
    pub(crate) fn operation(&self, completion_id: CompletionId) -> Option<OperationId> {
        self.by_completion.get(&completion_id).copied()
    }

    /// Returns the original paired deadline bound at public admission.
    pub(crate) fn deadline(&self, operation_id: OperationId) -> Option<OperationDeadline> {
        self.by_operation
            .get(&operation_id)
            .map(|binding| binding.deadline)
    }

    /// Removes an operation association and returns its completion generation.
    pub(crate) fn remove(
        &mut self,
        operation_id: OperationId,
    ) -> Result<CompletionId, OperationBindingError> {
        let binding = self
            .by_operation
            .remove(&operation_id)
            .ok_or(OperationBindingError::UnknownOperation)?;
        if binding.waiting_terminal {
            self.waiting_terminal = self
                .waiting_terminal
                .checked_sub(1)
                .unwrap_or_else(|| unreachable!("removed waiting terminal was counted"));
        }
        let completion_id = binding.completion_id;
        let removed = self.by_completion.remove(&completion_id);
        debug_assert!(removed.is_some());
        Ok(completion_id)
    }

    /// Removes only the exact operation and completion generation association.
    pub(crate) fn remove_exact(
        &mut self,
        operation_id: OperationId,
        completion_id: CompletionId,
    ) -> Result<(), OperationBindingError> {
        let Some(binding) = self.by_operation.get(&operation_id) else {
            return Err(OperationBindingError::UnknownOperation);
        };
        if binding.completion_id != completion_id {
            return Err(OperationBindingError::CompletionMismatch);
        }
        let removed_operation = self.by_operation.remove(&operation_id);
        if removed_operation.is_some_and(|binding| binding.waiting_terminal) {
            self.waiting_terminal = self
                .waiting_terminal
                .checked_sub(1)
                .unwrap_or_else(|| unreachable!("removed waiting terminal was counted"));
        }
        let removed = self.by_completion.remove(&completion_id);
        debug_assert!(removed_operation.is_some());
        debug_assert!(removed.is_some());
        Ok(())
    }

    /// Drops terminal-only associations after observer publication.
    pub(crate) fn clear_terminal(&mut self) {
        self.by_operation.clear();
        self.by_completion.clear();
        self.waiting_terminal = 0;
    }

    /// Returns the number of live operation associations.
    pub(crate) fn len(&self) -> usize {
        self.by_operation.len()
    }

    #[cfg(test)]
    pub(crate) fn allocation_capacity(&self) -> usize {
        self.by_operation.capacity()
    }
}
