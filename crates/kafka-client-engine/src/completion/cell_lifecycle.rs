//! Generation-fenced activation, admission rollback, and terminal recycling.

use std::sync::TryLockError;

use crate::completion::{
    CompletionId, CompletionRegistryError,
    cell::CompletionCell,
    state::{CellPhase, Presence},
};

impl<T> CompletionCell<T> {
    pub(super) fn activate(&self) -> Result<CompletionId, CompletionRegistryError> {
        let mut phase = self.lock();
        let CellPhase::Vacant { generation } = *phase else {
            return Err(CompletionRegistryError::UnknownCompletion);
        };
        let id = CompletionId::new(self.slot, generation);
        *phase = CellPhase::Pending {
            id,
            presence: Presence::Active,
            waker: None,
        };
        Ok(id)
    }

    pub(super) fn rollback_reservation(
        &self,
        id: CompletionId,
    ) -> Result<(), CompletionRegistryError> {
        let mut phase = self.lock();
        if !matches!(
            &*phase,
            CellPhase::Pending {
                id: current,
                presence: Presence::Active,
                waker: None,
            } if *current == id
        ) {
            return Err(CompletionRegistryError::UnknownCompletion);
        }
        advance_generation(&mut phase, id)
    }

    pub(super) fn try_recycle(&self, id: CompletionId) -> Result<bool, CompletionRegistryError> {
        let mut phase = match self.phase.try_lock() {
            Ok(phase) => phase,
            Err(TryLockError::WouldBlock) => return Ok(false),
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
        };
        if !matches!(&*phase, CellPhase::ReclaimQueued { id: current } if *current == id) {
            return Err(CompletionRegistryError::UnknownCompletion);
        }
        advance_generation(&mut phase, id)?;
        Ok(true)
    }
}

fn advance_generation<T>(
    phase: &mut CellPhase<T>,
    id: CompletionId,
) -> Result<(), CompletionRegistryError> {
    let Some(generation) = id.generation().checked_add(1) else {
        *phase = CellPhase::Retired;
        return Err(CompletionRegistryError::GenerationExhausted);
    };
    *phase = CellPhase::Vacant { generation };
    Ok(())
}
