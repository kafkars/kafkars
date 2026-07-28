//! Host-retained success payload that signals idle loss when unobserved.

use std::{
    sync::{
        Arc,
        atomic::AtomicBool,
        mpsc::{SyncSender, TrySendError},
    },
    time::Duration,
};

use kafka_client_core::TransactionalOwnerId;

use super::{
    TransactionInitializationOutcome, TransactionLifecycleControlPort, TransactionOwnerLossSignal,
    TransactionalOwnerHandle, outcome::TransactionInitializationFailure,
};

pub(in crate::transaction) enum RetainedTransactionInitializationOutcome {
    Initialized(RetainedTransactionalOwner),
    Failed(TransactionInitializationFailure),
}

pub(in crate::transaction) struct RetainedTransactionalOwner {
    owner_id: TransactionalOwnerId,
    transactional_id: Option<Arc<str>>,
    producer_id: i64,
    producer_epoch: i16,
    active: Arc<AtomicBool>,
    owner_loss: SyncSender<TransactionOwnerLossSignal>,
    owner_loss_timeout: Duration,
    armed: bool,
}

impl RetainedTransactionInitializationOutcome {
    pub(super) fn initialized(
        owner_id: TransactionalOwnerId,
        transactional_id: Arc<str>,
        producer_id: i64,
        producer_epoch: i16,
        active: Arc<AtomicBool>,
        owner_loss: SyncSender<TransactionOwnerLossSignal>,
        owner_loss_timeout: Duration,
    ) -> Self {
        Self::Initialized(RetainedTransactionalOwner {
            owner_id,
            transactional_id: Some(transactional_id),
            producer_id,
            producer_epoch,
            active,
            owner_loss,
            owner_loss_timeout,
            armed: true,
        })
    }

    pub(super) const fn is_initialized(&self) -> bool {
        matches!(self, Self::Initialized(_))
    }

    pub(super) fn into_observed(
        self,
        lifetime: Arc<dyn Send + Sync>,
        control: TransactionLifecycleControlPort,
    ) -> TransactionInitializationOutcome {
        match self {
            Self::Initialized(owner) => {
                TransactionInitializationOutcome::Initialized(owner.into_handle(lifetime, control))
            }
            Self::Failed(failure) => {
                drop((lifetime, control));
                TransactionInitializationOutcome::Failed(failure)
            }
        }
    }
}

impl RetainedTransactionalOwner {
    fn into_handle(
        mut self,
        lifetime: Arc<dyn Send + Sync>,
        control: TransactionLifecycleControlPort,
    ) -> TransactionalOwnerHandle {
        self.armed = false;
        TransactionalOwnerHandle::new(
            self.owner_id,
            self.transactional_id
                .take()
                .unwrap_or_else(|| unreachable!("retained owner keeps its transactional ID")),
            self.producer_id,
            self.producer_epoch,
            Arc::clone(&self.active),
            control,
            self.owner_loss_timeout,
            lifetime,
        )
    }
}

impl Drop for RetainedTransactionalOwner {
    fn drop(&mut self) {
        if self.armed {
            let signal = TransactionOwnerLossSignal {
                owner_id: self.owner_id,
                deadline: None,
            };
            match self.owner_loss.try_send(signal) {
                Ok(()) | Err(TrySendError::Disconnected(_)) => {}
                Err(TrySendError::Full(_)) => {
                    debug_assert!(false, "owner-loss capacity equals owner capacity");
                }
            }
        }
    }
}
