//! Fallible startup acquisition for bounded batch membership indexes.

use std::collections::TryReserveError;

use super::BatchStore;

impl BatchStore {
    pub(in crate::producer) fn try_new(
        batches: usize,
        members: usize,
    ) -> Result<Self, TryReserveError> {
        let mut store = Self::new(batches);
        store.batches.try_reserve(batches)?;
        store.operations.try_reserve(members)?;
        store.payloads.try_reserve(members)?;
        Ok(store)
    }

    #[cfg(test)]
    pub(in crate::producer) fn allocation_capacities(&self) -> (usize, usize, usize) {
        (
            self.batches.capacity(),
            self.operations.capacity(),
            self.payloads.capacity(),
        )
    }
}
