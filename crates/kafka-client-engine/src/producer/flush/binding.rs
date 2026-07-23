//! Fixed-capacity association of flush identities with completion generations.

use std::{error::Error, fmt};

use kafka_client_core::FlushId;

use crate::completion::CompletionId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FlushBinding {
    flush_id: FlushId,
    completion_id: CompletionId,
}

/// Failure to mutate producer flush completion ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FlushBindingError {
    Full,
    DuplicateFlush,
    DuplicateCompletion,
    UnknownFlush,
    CompletionMismatch,
}

impl fmt::Display for FlushBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Full => "producer flush completion bindings are full",
            Self::DuplicateFlush => "producer flush already owns a completion",
            Self::DuplicateCompletion => "engine completion already belongs to a flush",
            Self::UnknownFlush => "producer flush owns no completion binding",
            Self::CompletionMismatch => "producer flush owns a different completion generation",
        })
    }
}

impl Error for FlushBindingError {}

/// Linear bounded owner of flush-to-completion associations.
#[derive(Debug)]
pub(crate) struct FlushBindings {
    capacity: usize,
    entries: Vec<FlushBinding>,
}

impl FlushBindings {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn bind(
        &mut self,
        flush_id: FlushId,
        completion_id: CompletionId,
    ) -> Result<(), FlushBindingError> {
        let index = match self.flush_index(flush_id) {
            Ok(_) => return Err(FlushBindingError::DuplicateFlush),
            Err(index) => index,
        };
        if self
            .entries
            .iter()
            .any(|binding| binding.completion_id == completion_id)
        {
            return Err(FlushBindingError::DuplicateCompletion);
        }
        if self.entries.len() >= self.capacity {
            return Err(FlushBindingError::Full);
        }
        self.entries.insert(
            index,
            FlushBinding {
                flush_id,
                completion_id,
            },
        );
        Ok(())
    }

    pub(crate) fn completion(&self, flush_id: FlushId) -> Option<CompletionId> {
        self.flush_index(flush_id)
            .ok()
            .map(|index| self.entries[index].completion_id)
    }

    pub(crate) fn flush(&self, completion_id: CompletionId) -> Option<FlushId> {
        self.entries
            .iter()
            .find(|binding| binding.completion_id == completion_id)
            .map(|binding| binding.flush_id)
    }

    pub(crate) fn remove_exact(
        &mut self,
        flush_id: FlushId,
        completion_id: CompletionId,
    ) -> Result<(), FlushBindingError> {
        let index = self
            .flush_index(flush_id)
            .map_err(|_| FlushBindingError::UnknownFlush)?;
        if self.entries[index].completion_id != completion_id {
            return Err(FlushBindingError::CompletionMismatch);
        }
        self.entries.remove(index);
        Ok(())
    }

    pub(crate) fn clear_terminal(&mut self) {
        self.entries.clear();
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    fn flush_index(&self, flush_id: FlushId) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&flush_id, |binding| binding.flush_id)
    }
}
