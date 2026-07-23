//! Sole owner of bounded pending slots, byte accounting, and ordering.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use kafka_client_core::Deadline;

use super::{
    PendingAdmission, PendingAdmissionId, PendingNotificationPermitPool, PendingRegistryError,
};

mod mutation;
pub(super) use mutation::{PendingRemovalFailure, PendingRemovalPlan};

#[derive(Debug)]
pub(super) struct PendingSlot {
    pub(super) generation: u64,
    pub(super) entry: Option<PendingAdmission>,
}

/// Observable bounded pending-admission accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingAdmissionStats {
    pub(crate) records: usize,
    pub(crate) retained_bytes: usize,
    pub(crate) notification_permits: usize,
    pub(crate) accepting: bool,
}

/// Fixed-capacity owner of records waiting to attempt core admission.
#[derive(Debug)]
pub(crate) struct PendingAdmissionRegistry {
    pub(super) max_bytes: usize,
    pub(super) used_bytes: usize,
    pub(super) accepting: bool,
    pub(super) notification_permits: Arc<PendingNotificationPermitPool>,
    next_sequence: Option<u64>,
    pub(super) slots: Vec<PendingSlot>,
    pub(super) free: Vec<usize>,
    pub(super) fifo: BTreeMap<u64, PendingAdmissionId>,
    pub(super) deadlines: BTreeSet<(Deadline, u64, PendingAdmissionId)>,
    #[cfg(test)]
    pub(super) fail_next_restore_rollback: bool,
}

impl PendingAdmissionRegistry {
    pub(crate) fn with_notification_permits(
        max_records: usize,
        max_bytes: usize,
        notification_permits: Arc<PendingNotificationPermitPool>,
    ) -> Self {
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
            notification_permits,
            next_sequence: Some(0),
            slots,
            free,
            fifo: BTreeMap::new(),
            deadlines: BTreeSet::new(),
            #[cfg(test)]
            fail_next_restore_rollback: false,
        }
    }

    /// Removes exactly one live generation when cancellation wins.
    pub(crate) fn cancel(
        &mut self,
        id: PendingAdmissionId,
    ) -> Result<PendingAdmission, PendingRegistryError> {
        let slot = self
            .slots
            .get(id.slot())
            .ok_or(PendingRegistryError::StaleGeneration)?;
        if slot.generation != id.generation() {
            return Err(PendingRegistryError::StaleGeneration);
        }
        let pending = slot
            .entry
            .as_ref()
            .ok_or(PendingRegistryError::StaleGeneration)?;
        if !pending.is_abandoned() {
            return Err(PendingRegistryError::ObservationState);
        }
        self.remove(id)
    }

    /// Closes new registration before shutdown draining begins.
    pub(crate) fn begin_close(&mut self) {
        self.accepting = false;
    }

    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.deadlines.first().map(|(deadline, _, _)| *deadline)
    }

    /// Reports whether an older admission or abandoned tombstone owns FIFO precedence.
    pub(crate) fn has_entries(&self) -> bool {
        !self.fifo.is_empty()
    }

    pub(crate) fn stats(&self) -> PendingAdmissionStats {
        PendingAdmissionStats {
            records: self.fifo.len(),
            retained_bytes: self.used_bytes,
            notification_permits: self.notification_permits.in_use(),
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

    #[cfg(test)]
    pub(super) fn insert_fifo_index_for_test(&mut self, sequence: u64, id: PendingAdmissionId) {
        self.fifo.insert(sequence, id);
    }
}
