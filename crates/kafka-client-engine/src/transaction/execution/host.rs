//! One installed transaction lifecycle behind its exact initialized owner.

use kafka_client_core::{
    ProducerRetryPolicy, TransactionEndMode, TransactionEpoch, TransactionLifecycleTerminal,
    TransactionalOwnerId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionObserver,
    transaction::{
        TransactionLifecycleHostError, initialization::TransactionalOwnerParts,
        lifecycle::TransactionLifecycleHost,
    },
};

/// Unique execution owner installed after producer identity initialization.
pub(crate) struct TransactionExecutionHost {
    pub(super) lifecycle: TransactionLifecycleHost,
}

impl TransactionExecutionHost {
    pub(in crate::transaction) fn try_new(
        parts: TransactionalOwnerParts,
        retry_policy: ProducerRetryPolicy,
    ) -> Result<Self, (TransactionLifecycleHostError, TransactionalOwnerParts)> {
        TransactionLifecycleHost::try_new(parts, retry_policy).map(|lifecycle| Self { lifecycle })
    }

    pub(crate) fn owns(&self, owner_id: TransactionalOwnerId) -> bool {
        self.lifecycle.owns(owner_id)
    }

    pub(crate) fn begin(&mut self) -> Result<TransactionEpoch, TransactionLifecycleHostError> {
        self.lifecycle.begin()
    }

    pub(crate) fn end(
        &mut self,
        epoch: TransactionEpoch,
        mode: TransactionEndMode,
        deadline: OperationDeadline,
    ) -> Result<CompletionObserver<TransactionLifecycleTerminal>, TransactionLifecycleHostError>
    {
        match mode {
            TransactionEndMode::Commit => self.lifecycle.commit(epoch, deadline),
            TransactionEndMode::Abort => self.lifecycle.abort(epoch, deadline),
        }
    }

    pub(crate) fn owner_lost(
        &mut self,
        deadline: OperationDeadline,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.lifecycle.owner_lost(deadline)
    }

    pub(crate) fn idle_owner_lost(&mut self) -> Result<(), TransactionLifecycleHostError> {
        self.lifecycle.idle_owner_lost()
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.lifecycle.next_deadline()
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.lifecycle.unsettled()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.lifecycle.is_closed()
    }
}
