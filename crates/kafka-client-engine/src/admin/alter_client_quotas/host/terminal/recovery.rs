//! Exact call-and-plan recovery after the unique driver has been destroyed.

use kafka_client_core::{
    AlterClientQuotasEffect, AlterClientQuotasInput, AlterClientQuotasState, DeliveryStatus, Moment,
};

use super::super::{AlterClientQuotasHandoff, AlterClientQuotasHost, AlterClientQuotasHostError};

impl AlterClientQuotasHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), AlterClientQuotasHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            if operation.rejected_submission.is_some() {
                return Err(AlterClientQuotasHostError::SubmissionMismatch);
            }
            let operation_id = operation.operation_id;
            let state = operation.machine.state();
            let handoff = operation.handoff;
            match (state, handoff) {
                (AlterClientQuotasState::Ready, _) => self.apply(
                    operation_id,
                    AlterClientQuotasInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (AlterClientQuotasState::AwaitingDriver, AlterClientQuotasHandoff::Untouched) => {
                    self.apply(operation_id, AlterClientQuotasInput::DriverRejected)?;
                }
                (AlterClientQuotasState::AwaitingDriver, AlterClientQuotasHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, AlterClientQuotasInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (AlterClientQuotasState::Submitted, AlterClientQuotasHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AlterClientQuotasState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(AlterClientQuotasHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), AlterClientQuotasHostError> {
        if self.operations[index].recovered_call.is_none() {
            let call = self.operations[index]
                .call
                .take()
                .ok_or(AlterClientQuotasHostError::InvalidHandoff)?;
            match call.recover_after_driver_shutdown() {
                Ok(recovered) => self.operations[index].recovered_call = Some(recovered),
                Err(call) => {
                    self.operations[index].call = Some(call);
                    return Err(AlterClientQuotasHostError::InvalidHandoff);
                }
            }
        }
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AlterClientQuotasHostError::InvalidHandoff)?;
        if self.operations[index].matches_recovered(recovered) {
            Ok(())
        } else {
            Err(AlterClientQuotasHostError::SubmissionMismatch)
        }
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), AlterClientQuotasHostError> {
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AlterClientQuotasHostError::InvalidHandoff)?;
        if !self.operations[index].matches_recovered(recovered) {
            return Err(AlterClientQuotasHostError::SubmissionMismatch);
        }
        let transition =
            self.operations[index]
                .machine
                .apply(AlterClientQuotasInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(AlterClientQuotasEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(AlterClientQuotasHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(AlterClientQuotasHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
