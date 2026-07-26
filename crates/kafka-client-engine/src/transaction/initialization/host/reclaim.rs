//! Observer reclamation and failed-terminal retained-byte release.

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::TransactionInitializationHost;
use crate::transaction::initialization::TransactionInitializationHostError;

impl TransactionInitializationHost {
    pub(super) fn reclaim_one(&mut self) -> Result<bool, TransactionInitializationHostError> {
        let completion_id = if let Some(id) = self.reclaim_pending {
            id
        } else {
            let Some(id) = self.completions.next_reclaim()? else {
                return Ok(false);
            };
            self.reclaim_pending = Some(id);
            id
        };
        match self.completions.finish_reclaim(completion_id) {
            Ok(ReclaimStatus::Retry) => Ok(false),
            Ok(ReclaimStatus::Reclaimed) | Err(CompletionRegistryError::GenerationExhausted) => {
                let index = self
                    .published_bytes
                    .iter()
                    .position(|(id, _bytes)| *id == completion_id)
                    .ok_or(TransactionInitializationHostError::ByteAccounting)?;
                let (_id, bytes) = self.published_bytes.swap_remove(index);
                self.retained_bytes = self
                    .retained_bytes
                    .checked_sub(bytes)
                    .ok_or(TransactionInitializationHostError::ByteAccounting)?;
                self.reclaim_pending = None;
                Ok(true)
            }
            Err(error) => Err(TransactionInitializationHostError::Completion(error)),
        }
    }
}
