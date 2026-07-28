//! Ordered interpretation of deterministic lifecycle effects.

use kafka_client_core::{
    TransactionEndMode, TransactionLifecycleEffect, TransactionLifecycleInput,
    TransactionLifecycleTerminal,
};

use crate::clock::OperationDeadline;

use super::host::{PendingEndOperation, TransactionLifecycleHost, TransactionLifecycleHostError};

impl TransactionLifecycleHost {
    pub(super) fn interpret(
        &mut self,
        effect: Option<TransactionLifecycleEffect>,
        owner_loss_deadline: Option<OperationDeadline>,
    ) -> Result<(), TransactionLifecycleHostError> {
        match effect {
            None => Ok(()),
            Some(TransactionLifecycleEffect::EndTransaction { .. }) => {
                self.prepare_end(owner_loss_deadline)
            }
            Some(TransactionLifecycleEffect::Complete {
                operation_id,
                terminal,
                ..
            }) => {
                let pending = self.pending()?;
                if pending.operation_id != Some(operation_id) {
                    return Err(TransactionLifecycleHostError::UnexpectedEffect);
                }
                pending.terminal = Some(terminal);
                Ok(())
            }
            Some(TransactionLifecycleEffect::EnterFatal {
                operation_id,
                owner_lost,
                ..
            }) => {
                if let Some(operation_id) = operation_id {
                    let pending = self.pending()?;
                    if pending.operation_id != Some(operation_id) {
                        return Err(TransactionLifecycleHostError::UnexpectedEffect);
                    }
                    pending.terminal = Some(TransactionLifecycleTerminal::Fatal);
                }
                if owner_lost {
                    let transition = self
                        .machine
                        .apply(self.owner_id()?, TransactionLifecycleInput::OwnerLost)?;
                    self.interpret(transition.into_effect(), None)?;
                }
                Ok(())
            }
            Some(TransactionLifecycleEffect::ReleaseOwner { .. }) => {
                if let Some(owner) = self.owner.take() {
                    owner.release();
                }
                self.pending_end = None;
                Ok(())
            }
            Some(TransactionLifecycleEffect::Began { .. }) => {
                Err(TransactionLifecycleHostError::UnexpectedEffect)
            }
        }
    }

    fn prepare_end(
        &mut self,
        owner_loss_deadline: Option<OperationDeadline>,
    ) -> Result<(), TransactionLifecycleHostError> {
        if let Some(pending) = self.pending_end.as_mut() {
            pending.ready = true;
            return Ok(());
        }
        let deadline =
            owner_loss_deadline.ok_or(TransactionLifecycleHostError::MissingEndOperation)?;
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
            ready: true,
            call: None,
            terminal: None,
            retry_not_before: None,
            retries_started: 0,
        });
        Ok(())
    }

    fn pending(&mut self) -> Result<&mut PendingEndOperation, TransactionLifecycleHostError> {
        self.pending_end
            .as_mut()
            .ok_or(TransactionLifecycleHostError::MissingEndOperation)
    }
}
