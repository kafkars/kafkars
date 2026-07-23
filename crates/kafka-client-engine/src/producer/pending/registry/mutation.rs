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

#[derive(Debug, Eq, PartialEq)]
pub(in crate::producer::pending) struct PendingRemovalPlan {
    id: PendingAdmissionId,
    sequence: u64,
    deadline: Deadline,
    next_used_bytes: usize,
}

#[must_use = "failed removal retains the linear proof for recovery"]
pub(in crate::producer::pending) struct PendingRemovalFailure {
    error: PendingRegistryError,
    plan: PendingRemovalPlan,
}

impl PendingRemovalPlan {
    pub(in crate::producer::pending) const fn restored(
        id: PendingAdmissionId,
        sequence: u64,
        deadline: Deadline,
        next_used_bytes: usize,
    ) -> Self {
        Self {
            id,
            sequence,
            deadline,
            next_used_bytes,
        }
    }

    #[cfg(test)]
    pub(in crate::producer::pending) const fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl PendingRemovalFailure {
    pub(in crate::producer::pending) const fn error(&self) -> PendingRegistryError {
        self.error
    }

    pub(in crate::producer::pending) const fn into_parts(
        self,
    ) -> (PendingRegistryError, PendingRemovalPlan) {
        (self.error, self.plan)
    }
}

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
        let Some(permit) = self.notification_permits.reserve() else {
            return Err(rejected(
                PendingAdmissionRejectionReason::NotificationBackpressure,
                record,
            ));
        };
        let Some(id) = self.reserve_identity() else {
            permit.release();
            return Err(rejected(
                PendingAdmissionRejectionReason::IdentityExhausted,
                record,
            ));
        };
        let cell = PendingSendCell::new(permit);
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

    pub(in crate::producer::pending) fn remove(
        &mut self,
        id: PendingAdmissionId,
    ) -> Result<PendingAdmission, PendingRegistryError> {
        let plan = self.validate_remove(id)?;
        self.commit_remove(plan).map_err(|failure| failure.error())
    }

    pub(in crate::producer::pending) fn validate_remove(
        &self,
        id: PendingAdmissionId,
    ) -> Result<PendingRemovalPlan, PendingRegistryError> {
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
        Ok(PendingRemovalPlan {
            id,
            sequence,
            deadline: entry.deadline(),
            next_used_bytes: next_used,
        })
    }

    pub(in crate::producer::pending) fn commit_remove(
        &mut self,
        plan: PendingRemovalPlan,
    ) -> Result<PendingAdmission, PendingRemovalFailure> {
        let current = match self.validate_remove(plan.id) {
            Ok(current) => current,
            Err(error) => return Err(PendingRemovalFailure { error, plan }),
        };
        if current != plan {
            return Err(PendingRemovalFailure {
                error: PendingRegistryError::CorruptIndex,
                plan,
            });
        }
        let Some(entry) = self.slots[plan.id.slot()].entry.take() else {
            return Err(PendingRemovalFailure {
                error: PendingRegistryError::CorruptIndex,
                plan,
            });
        };
        self.fifo.remove(&plan.sequence);
        self.deadlines
            .remove(&(plan.deadline, plan.sequence, plan.id));
        self.used_bytes = plan.next_used_bytes;
        self.free.push(plan.id.slot());
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
