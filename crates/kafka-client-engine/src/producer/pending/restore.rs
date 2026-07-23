//! Atomic return of one promotion lease to its original FIFO position.

use super::{
    PendingAdmission, PendingAdmissionId, PendingAdmissionRegistry, PendingLocalFailure,
    PendingLocalFailureKind, PendingRegistryError, PendingRestoreFailure, PendingRestoreOutcome,
};

struct RestorePlan {
    id: PendingAdmissionId,
    free_position: usize,
    next_used_bytes: usize,
}

impl PendingAdmissionRegistry {
    /// Restores one failed healthy promotion without changing its identity.
    ///
    /// Validation is read-only. Every failure returns the exact pending entry.
    /// Once close begins, unadmitted work settles locally rather than becoming
    /// queued shutdown work again.
    #[allow(
        clippy::result_large_err,
        reason = "failed restoration must return the intact linear pending entry"
    )]
    pub(crate) fn restore_front(
        &mut self,
        pending: PendingAdmission,
    ) -> Result<PendingRestoreOutcome, PendingRestoreFailure> {
        if !self.accepting {
            return Ok(PendingRestoreOutcome::Shutdown(PendingLocalFailure::new(
                PendingLocalFailureKind::Shutdown,
                pending,
            )));
        }
        let plan = match self.validate_restore(&pending) {
            Ok(plan) => plan,
            Err(error) => return Err(PendingRestoreFailure::new(error, pending)),
        };
        self.commit_restore(&plan, pending);
        Ok(PendingRestoreOutcome::Restored)
    }

    fn validate_restore(
        &self,
        pending: &PendingAdmission,
    ) -> Result<RestorePlan, PendingRegistryError> {
        let id = pending.id();
        let slot = self
            .slots
            .get(id.slot())
            .ok_or(PendingRegistryError::UnknownSlot)?;
        if slot.generation != id.generation() {
            return Err(PendingRegistryError::StaleGeneration);
        }
        if slot.entry.is_some() {
            return Err(PendingRegistryError::SlotOccupied);
        }
        let free_position = unique_free_position(&self.free, id.slot())?;
        let sequence = pending.sequence();
        if self.fifo.contains_key(&sequence)
            || self.fifo.values().any(|indexed| *indexed == id)
            || self.deadlines.iter().any(|(_, indexed_sequence, indexed)| {
                *indexed_sequence == sequence || *indexed == id
            })
        {
            return Err(PendingRegistryError::IndexCollision);
        }
        if self
            .fifo
            .first_key_value()
            .is_some_and(|(first, _)| *first < sequence)
        {
            return Err(PendingRegistryError::FifoPrecedence);
        }
        if self.fifo.len() >= self.slots.len() {
            return Err(PendingRegistryError::CountCapacity);
        }
        let next_used_bytes = self
            .used_bytes
            .checked_add(pending.retained_bytes())
            .ok_or(PendingRegistryError::RetainedSizeOverflow)?;
        if next_used_bytes > self.max_bytes {
            return Err(PendingRegistryError::ByteCapacity);
        }
        Ok(RestorePlan {
            id,
            free_position,
            next_used_bytes,
        })
    }

    fn commit_restore(&mut self, plan: &RestorePlan, pending: PendingAdmission) {
        let sequence = pending.sequence();
        let deadline = pending.deadline();
        self.free.remove(plan.free_position);
        self.used_bytes = plan.next_used_bytes;
        self.slots[plan.id.slot()].entry = Some(pending);
        self.fifo.insert(sequence, plan.id);
        self.deadlines.insert((deadline, sequence, plan.id));
    }
}

fn unique_free_position(free: &[usize], slot: usize) -> Result<usize, PendingRegistryError> {
    let mut matches = free
        .iter()
        .enumerate()
        .filter_map(|(position, candidate)| (*candidate == slot).then_some(position));
    let position = matches.next().ok_or(PendingRegistryError::VacancyIndex)?;
    if matches.next().is_some() {
        return Err(PendingRegistryError::VacancyIndex);
    }
    Ok(position)
}
