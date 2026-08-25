//! Fixed-deadline transaction-end admission through the initialized shard.

use std::time::{Duration, Instant};

use kafka_client_core::{
    TransactionEndMode, TransactionEpoch, TransactionLifecycleTerminal, TransactionalOwnerId,
};

use crate::{clock::OperationDeadline, completion::CompletionObserver};

use super::{
    TransactionLifecycleControlAccepted, TransactionLifecycleControlError,
    TransactionLifecycleControlPort,
};

impl TransactionLifecycleControlPort {
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
        self.end_with_deadline(owner_id, epoch, mode, deadline)
    }

    pub(crate) fn end_until(
        &self,
        owner_id: TransactionalOwnerId,
        epoch: TransactionEpoch,
        mode: TransactionEndMode,
        deadline: Instant,
    ) -> Result<
        TransactionLifecycleControlAccepted<CompletionObserver<TransactionLifecycleTerminal>>,
        TransactionLifecycleControlError,
    > {
        let capture = self
            .shared
            .clock()
            .capture_deadline_until(deadline)
            .map_err(|_error| TransactionLifecycleControlError::InvalidDeadline)?;
        if capture.deadline().is_elapsed_at(capture.now()) {
            return Err(TransactionLifecycleControlError::InvalidDeadline);
        }
        self.end_with_deadline(owner_id, epoch, mode, capture.operation_deadline())
    }

    fn end_with_deadline(
        &self,
        owner_id: TransactionalOwnerId,
        epoch: TransactionEpoch,
        mode: TransactionEndMode,
        deadline: OperationDeadline,
    ) -> Result<
        TransactionLifecycleControlAccepted<CompletionObserver<TransactionLifecycleTerminal>>,
        TransactionLifecycleControlError,
    > {
        let observer = self.shared.try_end(owner_id, epoch, mode, deadline)?;
        Ok(TransactionLifecycleControlAccepted {
            value: observer,
            wake_failed: self.shared.wake().request().is_err(),
        })
    }
}
