//! Sole owner of bounded pending slots, byte accounting, and ordering.

use std::collections::{BTreeMap, BTreeSet};

use kafka_client_core::{Deadline, Moment};

use super::{
    PendingAdmission, PendingAdmissionId, PendingLocalFailure, PendingLocalFailureKind,
    PendingRegistryError,
};

mod mutation;

#[derive(Debug)]
struct PendingSlot {
    generation: u64,
    entry: Option<PendingAdmission>,
}

/// Observable bounded pending-admission accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingAdmissionStats {
    pub(crate) records: usize,
    pub(crate) retained_bytes: usize,
    pub(crate) accepting: bool,
}

/// Fixed-capacity owner of records waiting to attempt core admission.
#[derive(Debug)]
pub(crate) struct PendingAdmissionRegistry {
    max_bytes: usize,
    used_bytes: usize,
    accepting: bool,
    next_sequence: Option<u64>,
    slots: Vec<PendingSlot>,
    free: Vec<usize>,
    fifo: BTreeMap<u64, PendingAdmissionId>,
    deadlines: BTreeSet<(Deadline, u64, PendingAdmissionId)>,
}

impl PendingAdmissionRegistry {
    pub(crate) fn new(max_records: usize, max_bytes: usize) -> Self {
        let slots = (0..max_records)
            .map(|_| PendingSlot {
                generation: 0,
                entry: None,
            })
            .collect();
        let free = (0..max_records).rev().collect();
        Self {
            max_bytes,
            used_bytes: 0,
            accepting: true,
            next_sequence: Some(0),
            slots,
            free,
            fifo: BTreeMap::new(),
            deadlines: BTreeSet::new(),
        }
    }

    /// Removes exactly one live generation when cancellation wins.
    pub(crate) fn cancel(
        &mut self,
        id: PendingAdmissionId,
    ) -> Result<PendingAdmission, PendingRegistryError> {
        self.remove(id)
    }

    /// Takes the oldest record for one bounded host promotion attempt.
    pub(crate) fn take_next(&mut self) -> Result<Option<PendingAdmission>, PendingRegistryError> {
        let Some((_sequence, id)) = self.fifo.first_key_value() else {
            return Ok(None);
        };
        self.remove(*id).map(Some)
    }

    /// Settles at most `limit` elapsed records without core identities.
    pub(crate) fn expire_due(
        &mut self,
        now: Moment,
        limit: usize,
    ) -> Result<Vec<PendingLocalFailure>, PendingRegistryError> {
        let mut expired = Vec::with_capacity(limit.min(self.fifo.len()));
        while expired.len() < limit {
            let Some((deadline, _sequence, id)) = self.deadlines.first().copied() else {
                break;
            };
            if !deadline.is_elapsed_at(now) {
                break;
            }
            expired.push(PendingLocalFailure::new(
                PendingLocalFailureKind::DeadlineElapsed,
                self.remove(id)?,
            ));
        }
        Ok(expired)
    }

    /// Closes new registration before shutdown draining begins.
    pub(crate) fn begin_close(&mut self) {
        self.accepting = false;
    }

    /// Settles at most `limit` queued records during bounded shutdown work.
    pub(crate) fn drain_closed(
        &mut self,
        limit: usize,
    ) -> Result<Vec<PendingLocalFailure>, PendingRegistryError> {
        if self.accepting {
            return Err(PendingRegistryError::StillOpen);
        }
        let mut drained = Vec::with_capacity(limit.min(self.fifo.len()));
        while drained.len() < limit {
            let Some(pending) = self.take_next()? else {
                break;
            };
            drained.push(PendingLocalFailure::new(
                PendingLocalFailureKind::Shutdown,
                pending,
            ));
        }
        Ok(drained)
    }

    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.deadlines.first().map(|(deadline, _, _)| *deadline)
    }

    pub(crate) fn stats(&self) -> PendingAdmissionStats {
        PendingAdmissionStats {
            records: self.fifo.len(),
            retained_bytes: self.used_bytes,
            accepting: self.accepting,
        }
    }

    #[cfg(test)]
    pub(super) fn set_vacant_generation_for_test(
        &mut self,
        slot: usize,
        generation: u64,
    ) -> Result<(), PendingRegistryError> {
        let target = self
            .slots
            .get_mut(slot)
            .ok_or(PendingRegistryError::UnknownSlot)?;
        if target.entry.is_some() {
            return Err(PendingRegistryError::CorruptIndex);
        }
        target.generation = generation;
        Ok(())
    }
}
