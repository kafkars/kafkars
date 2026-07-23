//! Atomic insertion, generation allocation, and removal of pending records.

use std::{sync::Arc, time::Instant};

use kafka_client_core::Deadline;

use crate::producer::ProducerRecord;

use super::{
    super::{
        PendingAdmission, PendingAdmissionId, PendingAdmissionRejected,
        PendingAdmissionRejectionReason, PendingRegistryError, PendingSendCell,
        PendingSendRegistration,
    },
    PendingAdmissionRegistry,
};
use crate::producer::boundary::ProducerSend;

impl PendingAdmissionRegistry {
    /// Retains one unadmitted record under exact count and byte bounds.
    #[allow(
        clippy::result_large_err,
        reason = "rejection must return the intact linear producer record"
    )]
    pub(crate) fn register(
        &mut self,
        record: ProducerRecord,
        deadline: Deadline,
        absolute_instant: Instant,
    ) -> Result<PendingSendRegistration, PendingAdmissionRejected> {
        if !self.accepting {
            return Err(rejected(PendingAdmissionRejectionReason::Closed, record));
        }
        if self.fifo.len() >= self.slots.len() {
            return Err(rejected(
                PendingAdmissionRejectionReason::CountCapacity,
                record,
            ));
        }
        let Ok(retained_bytes) = record.retained_bytes() else {
            return Err(rejected(
                PendingAdmissionRejectionReason::RetainedSizeOverflow,
                record,
            ));
        };
        let Some(next_used) = self.used_bytes.checked_add(retained_bytes) else {
            return Err(rejected(
                PendingAdmissionRejectionReason::RetainedSizeOverflow,
                record,
            ));
        };
        if next_used > self.max_bytes {
            return Err(rejected(
                PendingAdmissionRejectionReason::ByteCapacity,
                record,
            ));
        }
        let Some(sequence) = self.next_sequence else {
            return Err(rejected(
                PendingAdmissionRejectionReason::IdentityExhausted,
                record,
            ));
        };
        let Some(id) = self.reserve_identity() else {
            return Err(rejected(
                PendingAdmissionRejectionReason::IdentityExhausted,
                record,
            ));
        };
        let cell = PendingSendCell::new();
        let send = ProducerSend::from_pending(Arc::clone(&cell));
        let entry = PendingAdmission::new(
            id,
            record,
            deadline,
            absolute_instant,
            retained_bytes,
            sequence,
            cell,
        );
        self.next_sequence = sequence.checked_add(1);
        self.used_bytes = next_used;
        self.slots[id.slot()].entry = Some(entry);
        self.fifo.insert(sequence, id);
        self.deadlines.insert((deadline, sequence, id));
        Ok(PendingSendRegistration::new(id, send))
    }

    pub(super) fn remove(
        &mut self,
        id: PendingAdmissionId,
    ) -> Result<PendingAdmission, PendingRegistryError> {
        let slot = self
            .slots
            .get(id.slot())
            .ok_or(PendingRegistryError::UnknownSlot)?;
        if slot.generation != id.generation() || slot.entry.is_none() {
            return Err(PendingRegistryError::StaleGeneration);
        }
        let entry = slot
            .entry
            .as_ref()
            .ok_or(PendingRegistryError::CorruptIndex)?;
        let sequence = entry.sequence();
        let deadline_key = (entry.deadline(), sequence, id);
        if self.fifo.get(&sequence) != Some(&id) || !self.deadlines.contains(&deadline_key) {
            return Err(PendingRegistryError::CorruptIndex);
        }
        let next_used = self
            .used_bytes
            .checked_sub(entry.retained_bytes())
            .ok_or(PendingRegistryError::RetainedAccounting)?;
        self.fifo.remove(&sequence);
        self.deadlines.remove(&deadline_key);
        let entry = self.slots[id.slot()]
            .entry
            .take()
            .ok_or(PendingRegistryError::CorruptIndex)?;
        self.used_bytes = next_used;
        self.free.push(id.slot());
        Ok(entry)
    }

    fn reserve_identity(&mut self) -> Option<PendingAdmissionId> {
        while let Some(slot_index) = self.free.pop() {
            let slot = &mut self.slots[slot_index];
            let Some(generation) = slot.generation.checked_add(1) else {
                continue;
            };
            slot.generation = generation;
            return Some(PendingAdmissionId::new(slot_index, generation));
        }
        None
    }
}

fn rejected(
    reason: PendingAdmissionRejectionReason,
    record: ProducerRecord,
) -> PendingAdmissionRejected {
    PendingAdmissionRejected::new(reason, record)
}
