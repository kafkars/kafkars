//! Engine admission rollback for fixed completion-registry slots.

use super::{CompletionPublisher, CompletionRegistry};
use crate::completion::{CompletionId, CompletionRegistryError};

impl<T: Send + 'static, P: CompletionPublisher<T>> CompletionRegistry<T, P> {
    /// Proves that an identity still names its exact reserved generation.
    pub(crate) fn validate_reserved(
        &self,
        id: CompletionId,
    ) -> Result<(), CompletionRegistryError> {
        let Some(slot) = self.slots.get(id.slot()) else {
            return Err(CompletionRegistryError::UnknownCompletion);
        };
        if slot.is_reserved(id) {
            Ok(())
        } else {
            Err(slot.publish_error(id))
        }
    }

    /// Rolls back engine capacity when deterministic core admission rejects.
    pub(crate) fn rollback_reservation(
        &mut self,
        id: CompletionId,
    ) -> Result<(), CompletionRegistryError> {
        let Some(slot) = self.slots.get_mut(id.slot()) else {
            return Err(CompletionRegistryError::UnknownCompletion);
        };
        if !slot.is_reserved(id) {
            return Err(CompletionRegistryError::UnknownCompletion);
        }
        match slot.cell.rollback_reservation(id) {
            Ok(()) => {
                slot.vacate();
                self.free.push(id.slot());
                Ok(())
            }
            Err(CompletionRegistryError::GenerationExhausted) => {
                slot.retire();
                Err(CompletionRegistryError::GenerationExhausted)
            }
            Err(error) => Err(error),
        }
    }
}
