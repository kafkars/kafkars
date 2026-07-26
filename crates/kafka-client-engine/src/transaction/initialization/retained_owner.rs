//! Host-retained success payload without a strong engine-lifetime cycle.

use std::sync::{Arc, atomic::AtomicBool, mpsc::SyncSender};

use kafka_client_core::TransactionalOwnerId;

use super::{
    TransactionInitializationOutcome, TransactionalOwnerHandle,
    outcome::{TransactionInitializationFailure, release_owner},
};

pub(super) enum RetainedTransactionInitializationOutcome {
    Initialized(RetainedTransactionalOwner),
    Failed(TransactionInitializationFailure),
}

pub(super) struct RetainedTransactionalOwner {
    owner_id: TransactionalOwnerId,
    transactional_id: String,
    producer_id: i64,
    producer_epoch: i16,
    active: Arc<AtomicBool>,
    release: SyncSender<TransactionalOwnerId>,
    armed: bool,
}

impl RetainedTransactionInitializationOutcome {
    pub(super) fn initialized(
        owner_id: TransactionalOwnerId,
        transactional_id: String,
        producer_id: i64,
        producer_epoch: i16,
        active: Arc<AtomicBool>,
        release: SyncSender<TransactionalOwnerId>,
    ) -> Self {
        Self::Initialized(RetainedTransactionalOwner {
            owner_id,
            transactional_id,
            producer_id,
            producer_epoch,
            active,
            release,
            armed: true,
        })
    }

    pub(super) const fn is_initialized(&self) -> bool {
        matches!(self, Self::Initialized(_))
    }

    pub(super) fn into_observed(
        self,
        lifetime: Arc<dyn Send + Sync>,
    ) -> TransactionInitializationOutcome {
        match self {
            Self::Initialized(owner) => {
                TransactionInitializationOutcome::Initialized(owner.into_handle(lifetime))
            }
            Self::Failed(failure) => {
                drop(lifetime);
                TransactionInitializationOutcome::Failed(failure)
            }
        }
    }
}

impl RetainedTransactionalOwner {
    fn into_handle(mut self, lifetime: Arc<dyn Send + Sync>) -> TransactionalOwnerHandle {
        self.armed = false;
        TransactionalOwnerHandle::new(
            self.owner_id,
            std::mem::take(&mut self.transactional_id),
            self.producer_id,
            self.producer_epoch,
            Arc::clone(&self.active),
            self.release.clone(),
            lifetime,
        )
    }
}

impl Drop for RetainedTransactionalOwner {
    fn drop(&mut self) {
        if self.armed {
            release_owner(self.owner_id, &self.active, &self.release);
        }
    }
}
