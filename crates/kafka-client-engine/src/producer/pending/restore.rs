//! Registry-first restoration of one coordinated pending promotion attempt.

use super::{
    PendingAdmission, PendingAdmissionId, PendingAttemptStateError, PendingPromotionAttempt,
    PendingRecordTransferState, PendingRegistryError,
    cell::PromotionRestore,
    promotion::PendingPromotion,
    registry::{PendingAdmissionRegistry, PendingRemovalFailure, PendingRemovalPlan},
    restore_error::{
        PendingAttemptRestoreError, PendingAttemptRestoreFailure, PendingAttemptRestoreOutcome,
    },
};

struct RestorePlan {
    id: PendingAdmissionId,
    free_position: usize,
    previous_used_bytes: usize,
    next_used_bytes: usize,
}

impl PendingPromotionAttempt {
    /// Inserts exact record/index/accounting while the cell stays `Promoting`.
    ///
    /// Only after insertion succeeds may the cell return to `Pending`. If
    /// observer drop raced, the exact insertion is removed again.
    pub(crate) fn restore(
        self,
        registry: &mut PendingAdmissionRegistry,
    ) -> Result<PendingAttemptRestoreOutcome, PendingAttemptRestoreFailure> {
        if self.transfer != PendingRecordTransferState::Retained {
            return Err(PendingAttemptRestoreFailure::attempt(
                PendingAttemptRestoreError::State(PendingAttemptStateError::RecordNotRetained),
                self,
            ));
        }
        let Some(admission) = self.admission.as_ref() else {
            return Err(PendingAttemptRestoreFailure::attempt(
                PendingAttemptRestoreError::State(PendingAttemptStateError::Invariant),
                self,
            ));
        };
        let plan = match registry.validate_restore(admission) {
            Ok(plan) => plan,
            Err(error) => {
                return Err(PendingAttemptRestoreFailure::attempt(
                    PendingAttemptRestoreError::Registry(error),
                    self,
                ));
            }
        };
        let PendingPromotionAttempt {
            admission,
            facts: _,
            promotion,
            transfer: _,
        } = self;
        let Some(admission) = admission else {
            return Err(PendingAttemptRestoreFailure::attempt(
                PendingAttemptRestoreError::State(PendingAttemptStateError::Invariant),
                PendingPromotionAttempt {
                    admission: None,
                    facts: None,
                    promotion,
                    transfer: PendingRecordTransferState::Retained,
                },
            ));
        };
        let rollback = registry.commit_restore(&plan, admission);
        match promotion.restore() {
            Ok(PromotionRestore::Pending) => Ok(PendingAttemptRestoreOutcome::Restored),
            Ok(PromotionRestore::Abandoned) => commit_abandoned_rollback(registry, rollback)
                .map(PendingAttemptRestoreOutcome::Abandoned),
            Err((promotion, error)) => {
                match commit_cell_rollback(registry, rollback, error, promotion) {
                    Ok((admission, promotion)) => Err(PendingAttemptRestoreFailure::attempt(
                        PendingAttemptRestoreError::State(PendingAttemptStateError::Cell(error)),
                        PendingPromotionAttempt::new(admission, promotion),
                    )),
                    Err(failure) => Err(failure),
                }
            }
        }
    }
}

impl PendingAdmissionRegistry {
    fn validate_restore(
        &self,
        pending: &PendingAdmission,
    ) -> Result<RestorePlan, PendingRegistryError> {
        if !self.accepting {
            return Err(PendingRegistryError::Closed);
        }
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
            previous_used_bytes: self.used_bytes,
            next_used_bytes,
        })
    }

    fn commit_restore(
        &mut self,
        plan: &RestorePlan,
        pending: PendingAdmission,
    ) -> PendingRemovalPlan {
        let sequence = pending.sequence();
        let deadline = pending.deadline();
        self.free.remove(plan.free_position);
        self.used_bytes = plan.next_used_bytes;
        self.slots[plan.id.slot()].entry = Some(pending);
        self.fifo.insert(sequence, plan.id);
        self.deadlines.insert((deadline, sequence, plan.id));
        PendingRemovalPlan::restored(plan.id, sequence, deadline, plan.previous_used_bytes)
    }

    #[cfg(test)]
    pub(super) fn inject_restore_rollback_failure_for_test(&mut self) {
        self.fail_next_restore_rollback = true;
    }

    #[cfg(test)]
    fn corrupt_restore_rollback_for_test(&mut self, plan: &PendingRemovalPlan) {
        if std::mem::take(&mut self.fail_next_restore_rollback) {
            self.fifo.remove(&plan.sequence());
        }
    }
}

fn commit_abandoned_rollback(
    registry: &mut PendingAdmissionRegistry,
    plan: PendingRemovalPlan,
) -> Result<PendingAdmission, PendingAttemptRestoreFailure> {
    #[cfg(test)]
    registry.corrupt_restore_rollback_for_test(&plan);
    registry
        .commit_remove(plan)
        .map_err(|failure: PendingRemovalFailure| {
            let (registry_error, plan) = failure.into_parts();
            PendingAttemptRestoreFailure::rollback(None, registry_error, plan, None)
        })
}

fn commit_cell_rollback(
    registry: &mut PendingAdmissionRegistry,
    plan: PendingRemovalPlan,
    cell_error: super::PendingCellError,
    promotion: PendingPromotion,
) -> Result<(PendingAdmission, PendingPromotion), PendingAttemptRestoreFailure> {
    #[cfg(test)]
    registry.corrupt_restore_rollback_for_test(&plan);
    match registry.commit_remove(plan) {
        Ok(admission) => Ok((admission, promotion)),
        Err(failure) => {
            let (registry_error, plan) = failure.into_parts();
            Err(PendingAttemptRestoreFailure::rollback(
                Some(cell_error),
                registry_error,
                plan,
                Some(promotion),
            ))
        }
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
