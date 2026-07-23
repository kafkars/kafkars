//! Bounded flush identities, admission barriers, and terminal publication.

use core::fmt;
use std::collections::{BTreeMap, btree_map::Entry};

use crate::{OperationId, ProducerEffect, ProducerOperation, ProducerOperationState};

/// Stable identity for one accepted producer flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FlushId(u64);

impl FlushId {
    /// Creates an identity from its deterministic raw value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the process-local deterministic identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The next record-admission sequence captured by one flush call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AdmissionSequence(u128);

impl AdmissionSequence {
    /// Creates a sequence from its deterministic raw value.
    pub const fn from_raw(value: u128) -> Self {
        Self(value)
    }

    fn capture(next_operation_id: Option<OperationId>) -> Self {
        match next_operation_id {
            Some(id) => Self(u128::from(id.get())),
            None => Self(u128::from(u64::MAX) + 1),
        }
    }

    /// Returns the next sequence that was available at the flush boundary.
    pub const fn get(self) -> u128 {
        self.0
    }

    const fn contains(self, operation_id: OperationId) -> bool {
        (operation_id.get() as u128) < self.0
    }
}

/// Rejected flush-ledger transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushLedgerError {
    /// Every bounded flush completion slot is retained.
    Capacity,
    /// The deterministic flush identity domain is exhausted.
    IdentityExhausted,
    /// The named flush is not retained.
    UnknownFlush,
    /// The named flush has not reached terminal publication.
    NotCompleted,
}

impl fmt::Display for FlushLedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Capacity => "producer flush completion capacity is exhausted",
            Self::IdentityExhausted => "producer flush identity space is exhausted",
            Self::UnknownFlush => "producer flush is unknown",
            Self::NotCompleted => "producer flush has not completed",
        })
    }
}

impl std::error::Error for FlushLedgerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlushState {
    Pending,
    Completed,
}

/// One linear flush barrier retained through terminal-result reclamation.
#[derive(Debug)]
struct FlushSlot {
    barrier: AdmissionSequence,
    state: FlushState,
}

impl FlushSlot {
    fn complete(&mut self) {
        self.state = FlushState::Completed;
    }
}

/// Bounded deterministic owner of accepted flush completions.
#[derive(Debug)]
pub(crate) struct FlushLedger {
    capacity: usize,
    next_id: Option<FlushId>,
    slots: BTreeMap<FlushId, FlushSlot>,
}

impl FlushLedger {
    pub(crate) const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            next_id: Some(FlushId::from_raw(1)),
            slots: BTreeMap::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.slots
            .values()
            .filter(|slot| slot.state == FlushState::Pending)
            .count()
    }

    pub(crate) fn request(
        &mut self,
        next_operation_id: Option<OperationId>,
        operations: &BTreeMap<OperationId, ProducerOperation>,
    ) -> Result<Vec<ProducerEffect>, FlushLedgerError> {
        if self.slots.len() >= self.capacity {
            return Err(FlushLedgerError::Capacity);
        }
        let id = self.next_id.ok_or(FlushLedgerError::IdentityExhausted)?;
        let barrier = AdmissionSequence::capture(next_operation_id);
        let completed = barrier_is_settled(barrier, operations);
        let state = if completed {
            FlushState::Completed
        } else {
            FlushState::Pending
        };
        match self.slots.entry(id) {
            Entry::Occupied(_) => return Err(FlushLedgerError::IdentityExhausted),
            Entry::Vacant(slot) => {
                slot.insert(FlushSlot { barrier, state });
            }
        }
        self.next_id = id.get().checked_add(1).map(FlushId::from_raw);

        let mut effects = vec![ProducerEffect::AcceptFlush {
            flush_id: id,
            barrier,
        }];
        if completed {
            effects.push(ProducerEffect::CompleteFlush { flush_id: id });
        }
        Ok(effects)
    }

    pub(crate) fn settle_ready(
        &mut self,
        operations: &BTreeMap<OperationId, ProducerOperation>,
    ) -> Vec<ProducerEffect> {
        let mut effects = Vec::new();
        for (flush_id, slot) in &mut self.slots {
            if slot.state == FlushState::Pending && barrier_is_settled(slot.barrier, operations) {
                slot.complete();
                effects.push(ProducerEffect::CompleteFlush {
                    flush_id: *flush_id,
                });
            }
        }
        effects
    }

    pub(crate) fn reclaim(&mut self, flush_id: FlushId) -> Result<(), FlushLedgerError> {
        match self.slots.get(&flush_id) {
            Some(FlushSlot {
                state: FlushState::Completed,
                ..
            }) => {
                self.slots.remove(&flush_id);
                Ok(())
            }
            Some(FlushSlot {
                state: FlushState::Pending,
                ..
            }) => Err(FlushLedgerError::NotCompleted),
            None => Err(FlushLedgerError::UnknownFlush),
        }
    }

    #[cfg(test)]
    pub(super) fn exhaust_identity(&mut self) {
        self.next_id = None;
    }
}

fn barrier_is_settled(
    barrier: AdmissionSequence,
    operations: &BTreeMap<OperationId, ProducerOperation>,
) -> bool {
    operations
        .iter()
        .filter(|(id, _operation)| barrier.contains(**id))
        .all(|(_id, operation)| operation.state() == ProducerOperationState::Completed)
}
