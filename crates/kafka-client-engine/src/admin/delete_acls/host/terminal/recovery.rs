//! Exact-call post-driver recovery for one ACL-deletion owner.

use kafka_client_core::{
    DeleteAclsEffect, DeleteAclsInput, DeleteAclsState, DeliveryStatus, Moment,
};

use super::super::{DeleteAclsHandoff, DeleteAclsHost, DeleteAclsHostError};

impl DeleteAclsHost {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), DeleteAclsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (DeleteAclsState::Ready, _) => self.apply(
                    operation_id,
                    DeleteAclsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (DeleteAclsState::AwaitingDriver, DeleteAclsHandoff::Untouched) => {
                    self.apply(operation_id, DeleteAclsInput::DriverRejected)?;
                }
                (DeleteAclsState::AwaitingDriver, DeleteAclsHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, DeleteAclsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (DeleteAclsState::Submitted, DeleteAclsHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (DeleteAclsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(DeleteAclsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), DeleteAclsHostError> {
        if self.operations[index].recovered_call.is_none()
            && let Some(call) = self.operations[index].call.take()
        {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(DeleteAclsHostError::InvalidHandoff)?;
        if self.operations[index].matches_recovered(recovered) {
            Ok(())
        } else {
            Err(DeleteAclsHostError::SubmissionMismatch)
        }
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), DeleteAclsHostError> {
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(DeleteAclsHostError::InvalidHandoff)?;
        if !self.operations[index].matches_recovered(recovered) {
            return Err(DeleteAclsHostError::SubmissionMismatch);
        }
        let transition =
            self.operations[index]
                .machine
                .apply(DeleteAclsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(DeleteAclsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(DeleteAclsHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DeleteAclsHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
