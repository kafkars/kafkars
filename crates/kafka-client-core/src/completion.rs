//! Bounded reservation and retention of terminal operation completions.

use core::fmt;
use std::collections::{BTreeMap, btree_map::Entry};

use crate::OperationId;

/// Rejected completion-ledger transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionLedgerError {
    /// No more completion slots can be reserved.
    Full,
    /// The operation identity already owns a slot.
    DuplicateOperation,
    /// The operation identity owns no slot.
    UnknownOperation,
    /// A terminal completion was already retained for the operation.
    AlreadyCompleted,
    /// The operation is admitted but has not reached a terminal completion.
    NotReady,
}

impl fmt::Display for CompletionLedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => formatter.write_str("completion ledger is full"),
            Self::DuplicateOperation => {
                formatter.write_str("operation already owns a completion slot")
            }
            Self::UnknownOperation => formatter.write_str("operation owns no completion slot"),
            Self::AlreadyCompleted => {
                formatter.write_str("operation already has a terminal completion")
            }
            Self::NotReady => formatter.write_str("operation has not completed"),
        }
    }
}

impl std::error::Error for CompletionLedgerError {}

/// Bounded terminal-completion storage reserved before operation admission.
#[derive(Debug)]
pub struct CompletionLedger<T> {
    capacity: usize,
    slots: BTreeMap<OperationId, Option<T>>,
}

impl<T> CompletionLedger<T> {
    /// Creates an empty ledger with a hard operation-count limit.
    pub const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            slots: BTreeMap::new(),
        }
    }

    /// Returns the configured slot count.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns reserved slots, including completed results not yet observed.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Returns whether the ledger contains no reserved slots.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Reserves terminal-completion capacity before admission.
    pub fn reserve(&mut self, id: OperationId) -> Result<(), CompletionLedgerError> {
        let has_capacity = self.slots.len() < self.capacity;
        match self.slots.entry(id) {
            Entry::Occupied(_) => Err(CompletionLedgerError::DuplicateOperation),
            Entry::Vacant(_) if !has_capacity => Err(CompletionLedgerError::Full),
            Entry::Vacant(slot) => {
                slot.insert(None);
                Ok(())
            }
        }
    }

    /// Retains exactly one terminal completion for a reserved operation.
    pub fn complete(&mut self, id: OperationId, value: T) -> Result<(), CompletionLedgerError> {
        let Some(slot) = self.slots.get_mut(&id) else {
            return Err(CompletionLedgerError::UnknownOperation);
        };
        if slot.is_some() {
            return Err(CompletionLedgerError::AlreadyCompleted);
        }
        *slot = Some(value);
        Ok(())
    }

    /// Removes and returns a terminal completion, releasing its slot.
    pub fn take(&mut self, id: OperationId) -> Result<T, CompletionLedgerError> {
        match self.slots.remove(&id) {
            Some(Some(value)) => Ok(value),
            Some(None) => {
                self.slots.insert(id, None);
                Err(CompletionLedgerError::NotReady)
            }
            None => Err(CompletionLedgerError::UnknownOperation),
        }
    }
}
