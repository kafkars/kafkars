//! Exact destructive attempt recovery after unique driver destruction.

use kafka_client_core::{
    DeliveryStatus, Moment, RemoveConsumerGroupMembersEffect, RemoveConsumerGroupMembersInput,
    RemoveConsumerGroupMembersState,
};

use super::super::{
    RemoveConsumerGroupMembersHandoff, RemoveConsumerGroupMembersHost,
    RemoveConsumerGroupMembersHostError,
    recovered::{retain as retain_recovered, take as take_recovered},
};

impl RemoveConsumerGroupMembersHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), RemoveConsumerGroupMembersHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            if operation.rejected_submission.is_some() {
                return Err(RemoveConsumerGroupMembersHostError::SubmissionMismatch);
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (RemoveConsumerGroupMembersState::Ready, _) => self.apply(
                    operation_id,
                    RemoveConsumerGroupMembersInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    RemoveConsumerGroupMembersState::AwaitingDriver,
                    RemoveConsumerGroupMembersHandoff::Untouched,
                ) => self.apply(
                    operation_id,
                    RemoveConsumerGroupMembersInput::DriverRejected,
                )?,
                (
                    RemoveConsumerGroupMembersState::AwaitingDriver,
                    RemoveConsumerGroupMembersHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(
                        operation_id,
                        RemoveConsumerGroupMembersInput::DriverAccepted,
                    )?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    RemoveConsumerGroupMembersState::Submitted,
                    RemoveConsumerGroupMembersHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (RemoveConsumerGroupMembersState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(RemoveConsumerGroupMembersHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), RemoveConsumerGroupMembersHostError> {
        if self.operations[index].recovered_call.is_none() {
            let call = self.operations[index]
                .call
                .take()
                .ok_or(RemoveConsumerGroupMembersHostError::InvalidHandoff)?;
            match call.recover_after_driver_shutdown() {
                Ok(recovered) => retain_recovered(&mut self.operations[index], recovered),
                Err(call) => {
                    self.operations[index].call = Some(call);
                    return Err(RemoveConsumerGroupMembersHostError::InvalidHandoff);
                }
            }
        }
        let operation = &self.operations[index];
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(RemoveConsumerGroupMembersHostError::InvalidHandoff)?;
        if operation.matches_recovered(recovered) {
            Ok(())
        } else {
            Err(RemoveConsumerGroupMembersHostError::SubmissionMismatch)
        }
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), RemoveConsumerGroupMembersHostError> {
        let operation = self
            .operations
            .get(index)
            .ok_or(RemoveConsumerGroupMembersHostError::UnknownOperation)?;
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(RemoveConsumerGroupMembersHostError::InvalidHandoff)?;
        if !operation.matches_recovered(recovered) {
            return Err(RemoveConsumerGroupMembersHostError::SubmissionMismatch);
        }
        let transition = self.operations[index].machine.apply(
            RemoveConsumerGroupMembersInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let terminal = match transition.into_effect() {
            Some(RemoveConsumerGroupMembersEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(RemoveConsumerGroupMembersHostError::MissingTerminal),
        };
        let recovered = take_recovered(&mut self.operations[index])
            .ok_or(RemoveConsumerGroupMembersHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
