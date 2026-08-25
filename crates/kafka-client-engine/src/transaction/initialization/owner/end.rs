//! Fixed-deadline transaction-end operations for one initialized owner.

use std::time::{Duration, Instant};

use kafka_client_core::{TransactionEndMode, TransactionEpoch, TransactionLifecycleTerminal};

use super::TransactionalOwnerHandle;
use crate::{
    completion::CompletionObserver,
    transaction::initialization::{
        TransactionLifecycleControlAccepted, TransactionLifecycleControlError,
    },
};

impl TransactionalOwnerHandle {
    pub(crate) fn commit(
        &self,
        epoch: TransactionEpoch,
        timeout: Duration,
    ) -> Result<
        TransactionLifecycleControlAccepted<CompletionObserver<TransactionLifecycleTerminal>>,
        TransactionLifecycleControlError,
    > {
        self.control
            .end(self.owner_id, epoch, TransactionEndMode::Commit, timeout)
    }

    pub(crate) fn commit_until(
        &self,
        epoch: TransactionEpoch,
        deadline: Instant,
    ) -> Result<
        TransactionLifecycleControlAccepted<CompletionObserver<TransactionLifecycleTerminal>>,
        TransactionLifecycleControlError,
    > {
        self.control
            .end_until(self.owner_id, epoch, TransactionEndMode::Commit, deadline)
    }

    pub(crate) fn abort(
        &self,
        epoch: TransactionEpoch,
        timeout: Duration,
    ) -> Result<
        TransactionLifecycleControlAccepted<CompletionObserver<TransactionLifecycleTerminal>>,
        TransactionLifecycleControlError,
    > {
        self.control
            .end(self.owner_id, epoch, TransactionEndMode::Abort, timeout)
    }

    pub(crate) fn abort_until(
        &self,
        epoch: TransactionEpoch,
        deadline: Instant,
    ) -> Result<
        TransactionLifecycleControlAccepted<CompletionObserver<TransactionLifecycleTerminal>>,
        TransactionLifecycleControlError,
    > {
        self.control
            .end_until(self.owner_id, epoch, TransactionEndMode::Abort, deadline)
    }
}
