//! Exact-owner lifecycle control and queued public-owner loss.

use std::sync::mpsc::TryRecvError;

use kafka_client_core::{
    TransactionEndMode, TransactionEpoch, TransactionLifecycleTerminal, TransactionalOwnerId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionObserver,
    transaction::{
        TransactionExecutionSendAdmissionError,
        offset_commit::{
            TransactionOffsetCommitAccepted, TransactionOffsetCommitAdmissionError,
            TransactionOffsetCommitAdmissionErrorKind, TransactionOffsetCommitRequest,
        },
        send::{TransactionSendAccepted, TransactionSendInput},
    },
};

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

    pub(in crate::transaction::initialization) fn preflight_commit_lifecycle(
        &self,
        owner_id: TransactionalOwnerId,
        epoch: TransactionEpoch,
    ) -> Result<(), TransactionLifecycleControlError> {
        self.execution_ref(owner_id)?
            .preflight_commit(epoch)
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

    #[expect(
        clippy::result_large_err,
        reason = "host rejection returns the exact caller-owned transactional record"
    )]
    pub(in crate::transaction) fn try_send(
        &mut self,
        owner_id: TransactionalOwnerId,
        input: TransactionSendInput,
    ) -> Result<TransactionSendAccepted, TransactionExecutionSendAdmissionError> {
        let Some(execution) = self
            .executions
            .iter_mut()
            .find(|execution| execution.owns(owner_id))
        else {
            return Err(TransactionExecutionSendAdmissionError::new(
                crate::transaction::TransactionExecutionSendAdmissionErrorKind::StaleOwner,
                input,
            ));
        };
        execution.try_send(owner_id, input)
    }

    #[expect(
        clippy::result_large_err,
        reason = "host rejection returns the exact assignment-fenced offset request"
    )]
    pub(in crate::transaction) fn try_offset_commit(
        &mut self,
        owner_id: TransactionalOwnerId,
        input: TransactionOffsetCommitRequest,
    ) -> Result<TransactionOffsetCommitAccepted, TransactionOffsetCommitAdmissionError> {
        let Some(execution) = self
            .executions
            .iter_mut()
            .find(|execution| execution.owns(owner_id))
        else {
            return Err(TransactionOffsetCommitAdmissionError::new(
                TransactionOffsetCommitAdmissionErrorKind::StaleOwner,
                input,
            ));
        };
        execution.try_offset_commit(input)
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

    fn execution_ref(
        &self,
        owner_id: TransactionalOwnerId,
    ) -> Result<&crate::transaction::TransactionExecutionHost, TransactionLifecycleControlError>
    {
        self.executions
            .iter()
            .find(|execution| execution.owns(owner_id))
            .ok_or(TransactionLifecycleControlError::StaleOwner)
    }
}
