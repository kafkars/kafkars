//! Exact accepted-call recovery after the unique driver has been destroyed.

use kafka_client_core::{
    DeleteConsumerGroupOffsetsEffect, DeleteConsumerGroupOffsetsInput,
    DeleteConsumerGroupOffsetsState, DeliveryStatus, Moment,
};

use super::super::{
    DeleteConsumerGroupOffsetsHandoff, DeleteConsumerGroupOffsetsHost,
    DeleteConsumerGroupOffsetsHostError,
};

impl DeleteConsumerGroupOffsetsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (DeleteConsumerGroupOffsetsState::Ready, _) => self.apply(
                    operation_id,
                    DeleteConsumerGroupOffsetsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    DeleteConsumerGroupOffsetsState::AwaitingDriver,
                    DeleteConsumerGroupOffsetsHandoff::Untouched,
                ) => self.apply(
                    operation_id,
                    DeleteConsumerGroupOffsetsInput::DriverRejected,
                )?,
                (
                    DeleteConsumerGroupOffsetsState::AwaitingDriver,
                    DeleteConsumerGroupOffsetsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(
                        operation_id,
                        DeleteConsumerGroupOffsetsInput::DriverAccepted,
                    )?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    DeleteConsumerGroupOffsetsState::Submitted,
                    DeleteConsumerGroupOffsetsHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (DeleteConsumerGroupOffsetsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(DeleteConsumerGroupOffsetsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
        if self.operations[index].recovered_call.is_none()
            && let Some(call) = self.operations[index].call.take()
        {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let operation = &self.operations[index];
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(DeleteConsumerGroupOffsetsHostError::InvalidHandoff)?;
        if !recovered.matches_evidence(&operation.response_plan, operation.result_limit) {
            return Err(DeleteConsumerGroupOffsetsHostError::SubmissionMismatch);
        }
        Ok(())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
        {
            let operation = &self.operations[index];
            let recovered = operation
                .recovered_call
                .as_ref()
                .ok_or(DeleteConsumerGroupOffsetsHostError::InvalidHandoff)?;
            if !recovered.matches_evidence(&operation.response_plan, operation.result_limit) {
                return Err(DeleteConsumerGroupOffsetsHostError::SubmissionMismatch);
            }
        }
        let transition = self.operations[index].machine.apply(
            DeleteConsumerGroupOffsetsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let terminal = match transition.into_effect() {
            Some(DeleteConsumerGroupOffsetsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(DeleteConsumerGroupOffsetsHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DeleteConsumerGroupOffsetsHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
