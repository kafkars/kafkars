//! Unique idle-owner release and shutdown invalidation.

use std::sync::atomic::Ordering;

use super::TransactionInitializationHost;
use crate::transaction::initialization::TransactionInitializationHostError;

impl TransactionInitializationHost {
    pub(super) fn release_one_owner(&mut self) -> Result<bool, TransactionInitializationHostError> {
        let owner_id = match self.release_receiver.try_recv() {
            Ok(owner_id) => owner_id,
            Err(std::sync::mpsc::TryRecvError::Empty) => return Ok(false),
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                return Err(TransactionInitializationHostError::OwnerRelease);
            }
        };
        let index = self
            .live_owners
            .iter()
            .position(|owner| owner.owner_id == owner_id)
            .ok_or(TransactionInitializationHostError::OwnerRelease)?;
        let owner = self.live_owners.swap_remove(index);
        owner.active.store(false, Ordering::Release);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(owner.retained_bytes)
            .ok_or(TransactionInitializationHostError::ByteAccounting)?;
        Ok(true)
    }

    pub(super) fn invalidate_live_owners(
        &mut self,
    ) -> Result<(), TransactionInitializationHostError> {
        while let Some(owner) = self.live_owners.pop() {
            owner.active.store(false, Ordering::Release);
            self.retained_bytes = self
                .retained_bytes
                .checked_sub(owner.retained_bytes)
                .ok_or(TransactionInitializationHostError::ByteAccounting)?;
        }
        Ok(())
    }
}
