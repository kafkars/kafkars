//! Bounded reservation and terminal-readiness markers for operation completions.

use core::fmt;
use std::collections::hash_map::Entry;

use crate::{
    OperationId,
    id_hash::{IdMap, id_map},
};

/// Rejected completion-ledger transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionLedgerError {
    /// No more completion slots can be reserved.
    Full,
    /// The operation identity already owns a slot.
    DuplicateOperation,
    /// The operation identity owns no slot.
    UnknownOperation,
    /// A terminal decision was already marked for the operation.
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
                formatter.write_str("operation already has a terminal decision")
            }
            Self::NotReady => formatter.write_str("operation has not completed"),
        }
    }
}

impl std::error::Error for CompletionLedgerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionState {
    Pending,
    Terminal,
}

type CompletionSlots = IdMap<OperationId, CompletionState>;

/// Bounded terminal-completion capacity reserved before operation admission.
///
/// This ledger stores only lifecycle markers. The engine owns terminal result
/// payloads and their observer wakeups.
#[derive(Debug)]
pub struct CompletionLedger {
    capacity: usize,
    slots: CompletionSlots,
}

impl CompletionLedger {
    /// Creates an empty ledger with a hard operation-count limit.
    pub const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            slots: id_map(),
        }
    }

    /// Returns the configured slot count.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns reserved slots, including terminal results retained by the engine.
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
                slot.insert(CompletionState::Pending);
                Ok(())
            }
        }
    }

    /// Marks exactly one terminal decision for a reserved operation.
    pub fn mark_terminal(&mut self, id: OperationId) -> Result<(), CompletionLedgerError> {
        let Some(state) = self.slots.get_mut(&id) else {
            return Err(CompletionLedgerError::UnknownOperation);
        };
        if *state == CompletionState::Terminal {
            return Err(CompletionLedgerError::AlreadyCompleted);
        }
        *state = CompletionState::Terminal;
        Ok(())
    }

    pub(crate) fn require_pending(&self, ids: &[OperationId]) -> Result<(), CompletionLedgerError> {
        for id in ids {
            match self.slots.get(id) {
                Some(CompletionState::Pending) => {}
                Some(CompletionState::Terminal) => {
                    return Err(CompletionLedgerError::AlreadyCompleted);
                }
                None => return Err(CompletionLedgerError::UnknownOperation),
            }
        }
        Ok(())
    }

    pub(crate) fn commit_terminal_many(&mut self, ids: &[OperationId]) {
        for id in ids {
            let state = self.slots.get_mut(id);
            debug_assert!(state.is_some());
            if let Some(state) = state {
                *state = CompletionState::Terminal;
            }
        }
    }

    /// Releases a terminal marker after the engine reclaims its retained result.
    pub fn reclaim(&mut self, id: OperationId) -> Result<(), CompletionLedgerError> {
        match self.slots.get(&id) {
            Some(CompletionState::Terminal) => {
                self.slots.remove(&id);
                Ok(())
            }
            Some(CompletionState::Pending) => Err(CompletionLedgerError::NotReady),
            None => Err(CompletionLedgerError::UnknownOperation),
        }
    }
}
