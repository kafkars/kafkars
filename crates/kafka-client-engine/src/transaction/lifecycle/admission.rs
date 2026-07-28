//! Begin, send, explicit end, and owner-loss admission transitions.

use kafka_client_core::{
    OperationId, TransactionEndMode, TransactionEpoch, TransactionLifecycleEffect,
    TransactionLifecycleInput, TransactionLifecycleTerminal, TransactionSendId,
    TransactionSendOutcome,
};

use crate::{clock::OperationDeadline, completion::CompletionObserver};

use super::host::{PendingEndOperation, TransactionLifecycleHost, TransactionLifecycleHostError};

impl TransactionLifecycleHost {
    pub(crate) fn begin(&mut self) -> Result<TransactionEpoch, TransactionLifecycleHostError> {
        self.preflight_sequence_activation()?;
        self.enrollment.preflight_activate_epoch()?;
        let transition = self
            .machine
            .apply(self.owner_id()?, TransactionLifecycleInput::Begin)?;
        let Some(TransactionLifecycleEffect::Began { epoch, .. }) = transition.into_effect() else {
            return Err(TransactionLifecycleHostError::UnexpectedEffect);
        };
        if self.enrollment.activate_epoch(epoch).is_err() {
            unreachable!("successful enrollment preflight makes activation infallible");
        }
        if self.sequencing.activate(epoch).is_err() {
            unreachable!("successful sequencing preflight makes activation infallible");
        }
        Ok(epoch)
    }

    pub(crate) fn accept_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.apply(TransactionLifecycleInput::SendAccepted { epoch, send_id })
    }

    pub(crate) fn settle_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        outcome: TransactionSendOutcome,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.apply(TransactionLifecycleInput::SendSettled {
            epoch,
            send_id,
            outcome,
        })
    }

    pub(in crate::transaction) fn settle_offset_commit(
        &mut self,
        epoch: TransactionEpoch,
        consequence: kafka_client_core::TransactionOffsetCommitConsequence,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.apply(TransactionLifecycleInput::OffsetCommitSettled { epoch, consequence })
    }

    pub(crate) fn commit(
        &mut self,
        epoch: TransactionEpoch,
        deadline: OperationDeadline,
    ) -> Result<CompletionObserver<TransactionLifecycleTerminal>, TransactionLifecycleHostError>
    {
        self.admit_end(epoch, TransactionEndMode::Commit, deadline)
    }

    pub(crate) fn abort(
        &mut self,
        epoch: TransactionEpoch,
        deadline: OperationDeadline,
    ) -> Result<CompletionObserver<TransactionLifecycleTerminal>, TransactionLifecycleHostError>
    {
        self.admit_end(epoch, TransactionEndMode::Abort, deadline)
    }

    pub(crate) fn owner_lost(
        &mut self,
        deadline: OperationDeadline,
    ) -> Result<(), TransactionLifecycleHostError> {
        if self.pending_end.is_some() {
            self.release_after_end = true;
            return Ok(());
        }
        let transition = self
            .machine
            .apply(self.owner_id()?, TransactionLifecycleInput::OwnerLost)?;
        let effect = transition.into_effect();
        if matches!(
            effect,
            Some(TransactionLifecycleEffect::CancelOutstanding { .. })
        ) {
            let epoch = self
                .machine
                .active_epoch()
                .ok_or(TransactionLifecycleHostError::MissingEndOperation)?;
            self.pending_end = Some(PendingEndOperation {
                operation_id: None,
                completion_id: None,
                epoch,
                mode: TransactionEndMode::Abort,
                deadline,
                ready: false,
                call: None,
                terminal: None,
                retry_not_before: None,
                retries_started: 0,
            });
            return Ok(());
        }
        self.interpret(effect, Some(deadline))
    }

    pub(crate) fn idle_owner_lost(&mut self) -> Result<(), TransactionLifecycleHostError> {
        if self.machine.active_epoch().is_some() {
            return Err(TransactionLifecycleHostError::MissingEndOperation);
        }
        let transition = self
            .machine
            .apply(self.owner_id()?, TransactionLifecycleInput::OwnerLost)?;
        self.interpret(transition.into_effect(), None)
    }

    fn admit_end(
        &mut self,
        epoch: TransactionEpoch,
        mode: TransactionEndMode,
        deadline: OperationDeadline,
    ) -> Result<CompletionObserver<TransactionLifecycleTerminal>, TransactionLifecycleHostError>
    {
        if self.pending_end.is_some() {
            return Err(TransactionLifecycleHostError::MissingEndOperation);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(TransactionLifecycleHostError::OperationIdentityExhausted)?;
        let (completion_id, observer) = self.completions.reserve()?;
        let input = match mode {
            TransactionEndMode::Commit => TransactionLifecycleInput::Commit {
                epoch,
                operation_id,
            },
            TransactionEndMode::Abort => TransactionLifecycleInput::Abort {
                epoch,
                operation_id,
            },
        };
        let transition = match self.machine.apply(self.owner_id()?, input) {
            Ok(transition) => transition,
            Err(error) => {
                self.completions.rollback_reservation(completion_id)?;
                return Err(error.into());
            }
        };
        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.pending_end = Some(PendingEndOperation {
            operation_id: Some(operation_id),
            completion_id: Some(completion_id),
            epoch,
            mode,
            deadline,
            ready: false,
            call: None,
            terminal: None,
            retry_not_before: None,
            retries_started: 0,
        });
        self.interpret(transition.into_effect(), None)?;
        Ok(observer)
    }
}
