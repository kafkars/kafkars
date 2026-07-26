//! Exact accepted-call recovery after the unique coordinator driver is destroyed.

use kafka_client_core::{
    AlterConsumerGroupOffsetsEffect, AlterConsumerGroupOffsetsInput,
    AlterConsumerGroupOffsetsState, DeliveryStatus, Moment,
};

use super::super::{
    AlterConsumerGroupOffsetsHandoff, AlterConsumerGroupOffsetsHost,
    AlterConsumerGroupOffsetsHostError,
    recovered::{retain as retain_recovered_call, take as take_recovered_call},
};

impl AlterConsumerGroupOffsetsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), AlterConsumerGroupOffsetsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            if operation.rejected_submission.is_some() {
                return Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch);
            }
            let operation_id = operation.operation_id;
            let state = operation.machine.state();
            let handoff = operation.handoff;
            match (state, handoff) {
                (AlterConsumerGroupOffsetsState::Ready, _) => self.apply(
                    operation_id,
                    AlterConsumerGroupOffsetsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    AlterConsumerGroupOffsetsState::AwaitingDriver,
                    AlterConsumerGroupOffsetsHandoff::Untouched,
                ) => self.apply(operation_id, AlterConsumerGroupOffsetsInput::DriverRejected)?,
                (
                    AlterConsumerGroupOffsetsState::AwaitingDriver,
                    AlterConsumerGroupOffsetsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, AlterConsumerGroupOffsetsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    AlterConsumerGroupOffsetsState::Submitted,
                    AlterConsumerGroupOffsetsHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AlterConsumerGroupOffsetsState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(AlterConsumerGroupOffsetsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), AlterConsumerGroupOffsetsHostError> {
        if self.operations[index].recovered_call.is_none() {
            let call = self.operations[index]
                .call
                .take()
                .ok_or(AlterConsumerGroupOffsetsHostError::InvalidHandoff)?;
            match call.recover_after_driver_shutdown() {
                Ok(recovered) => {
                    retain_recovered_call(&mut self.operations[index], Some(recovered));
                }
                Err(call) => {
                    self.operations[index].call = Some(call);
                    return Err(AlterConsumerGroupOffsetsHostError::InvalidHandoff);
                }
            }
        }
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AlterConsumerGroupOffsetsHostError::InvalidHandoff)?;
        if self.operations[index].matches_recovered(recovered) {
            Ok(())
        } else {
            Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch)
        }
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), AlterConsumerGroupOffsetsHostError> {
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AlterConsumerGroupOffsetsHostError::InvalidHandoff)?;
        if !self.operations[index].matches_recovered(recovered) {
            return Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index].machine.apply(
            AlterConsumerGroupOffsetsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let terminal = match transition.into_effect() {
            Some(AlterConsumerGroupOffsetsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(AlterConsumerGroupOffsetsHostError::MissingTerminal),
        };
        let recovered = take_recovered_call(&mut self.operations[index])
            .ok_or(AlterConsumerGroupOffsetsHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
