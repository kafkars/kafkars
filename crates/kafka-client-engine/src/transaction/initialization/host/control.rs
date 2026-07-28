//! Exact-owner lifecycle control and queued public-owner loss.

use std::sync::mpsc::TryRecvError;

use kafka_client_core::{
    TransactionEndMode, TransactionEpoch, TransactionLifecycleTerminal, TransactionalOwnerId,
};

use crate::{clock::OperationDeadline, completion::CompletionObserver};

use super::TransactionInitializationHost;
use crate::transaction::initialization::{
    TransactionLifecycleControlError, TransactionOwnerLossSignal,
};

impl TransactionInitializationHost {
    pub(in crate::transaction::initialization) fn owner_loss_sender(
        &self,
    ) -> std::sync::mpsc::SyncSender<TransactionOwnerLossSignal> {
        self.owner_loss_sender.clone()
    }

    pub(in crate::transaction::initialization) fn begin_lifecycle(
        &mut self,
        owner_id: TransactionalOwnerId,
    ) -> Result<TransactionEpoch, TransactionLifecycleControlError> {
        self.execution(owner_id)?
            .begin()
            .map_err(TransactionLifecycleControlError::Host)
    }

    pub(in crate::transaction::initialization) fn end_lifecycle(
        &mut self,
        owner_id: TransactionalOwnerId,
        epoch: TransactionEpoch,
        mode: TransactionEndMode,
        deadline: OperationDeadline,
    ) -> Result<CompletionObserver<TransactionLifecycleTerminal>, TransactionLifecycleControlError>
    {
        self.execution(owner_id)?
            .end(epoch, mode, deadline)
            .map_err(TransactionLifecycleControlError::Host)
    }

    pub(super) fn owner_loss_one(
        &mut self,
    ) -> Result<bool, crate::transaction::initialization::TransactionInitializationHostError> {
        let signal = match self.owner_loss_receiver.try_recv() {
            Ok(signal) => signal,
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => return Ok(false),
        };
        let Some(execution) = self
            .executions
            .iter_mut()
            .find(|execution| execution.owns(signal.owner_id))
        else {
            return Ok(true);
        };
        match signal.deadline {
            Some(deadline) => execution.owner_lost(deadline)?,
            None => execution.idle_owner_lost()?,
        }
        Ok(true)
    }

    fn execution(
        &mut self,
        owner_id: TransactionalOwnerId,
    ) -> Result<&mut crate::transaction::TransactionExecutionHost, TransactionLifecycleControlError>
    {
        self.executions
            .iter_mut()
            .find(|execution| execution.owns(owner_id))
            .ok_or(TransactionLifecycleControlError::StaleOwner)
    }
}
