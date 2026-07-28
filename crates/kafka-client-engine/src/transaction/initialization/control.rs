//! Bounded shard control for one initialized transactional lifecycle.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    TransactionEndMode, TransactionEpoch, TransactionLifecycleTerminal, TransactionalOwnerId,
};

use crate::{clock::OperationDeadline, completion::CompletionObserver};

use super::shard::TransactionInitializationShardState;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionLifecycleControlError {
    InvalidDeadline,
    Contended,
    Closed,
    StaleOwner,
    Host(crate::transaction::TransactionLifecycleHostError),
}

pub(crate) struct TransactionLifecycleControlAccepted<T> {
    pub(crate) value: T,
    pub(crate) wake_failed: bool,
}

pub(crate) struct TransactionOwnerLossSignal {
    pub(super) owner_id: TransactionalOwnerId,
    pub(super) deadline: Option<OperationDeadline>,
}

#[derive(Clone)]
pub(crate) struct TransactionLifecycleControlPort {
    shared: Arc<TransactionInitializationShardState>,
}

impl TransactionLifecycleControlPort {
    pub(super) const fn new(shared: Arc<TransactionInitializationShardState>) -> Self {
        Self { shared }
    }

    pub(crate) fn begin(
        &self,
        owner_id: TransactionalOwnerId,
    ) -> Result<
        TransactionLifecycleControlAccepted<TransactionEpoch>,
        TransactionLifecycleControlError,
    > {
        let epoch = self.shared.try_begin(owner_id)?;
        Ok(TransactionLifecycleControlAccepted {
            value: epoch,
            wake_failed: self.shared.wake().request().is_err(),
        })
    }

    pub(crate) fn end(
        &self,
        owner_id: TransactionalOwnerId,
        epoch: TransactionEpoch,
        mode: TransactionEndMode,
        timeout: Duration,
    ) -> Result<
        TransactionLifecycleControlAccepted<CompletionObserver<TransactionLifecycleTerminal>>,
        TransactionLifecycleControlError,
    > {
        let deadline = self
            .shared
            .clock()
            .capture_deadline_after(timeout)
            .ok()
            .filter(|_| !timeout.is_zero())
            .map(crate::clock::DeadlineCapture::operation_deadline)
            .ok_or(TransactionLifecycleControlError::InvalidDeadline)?;
        let observer = self.shared.try_end(owner_id, epoch, mode, deadline)?;
        Ok(TransactionLifecycleControlAccepted {
            value: observer,
            wake_failed: self.shared.wake().request().is_err(),
        })
    }

    pub(super) fn owner_lost(&self, owner_id: TransactionalOwnerId, timeout: Duration) {
        let deadline = self
            .shared
            .clock()
            .capture_deadline_after(timeout)
            .ok()
            .map(crate::clock::DeadlineCapture::operation_deadline);
        self.shared
            .enqueue_owner_loss(TransactionOwnerLossSignal { owner_id, deadline });
    }
}
