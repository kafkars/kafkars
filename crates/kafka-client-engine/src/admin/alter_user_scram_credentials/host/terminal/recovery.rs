//! Exact accepted-call and correlation-plan recovery after driver destruction.

use kafka_client_core::{
    AlterUserScramCredentialsEffect, AlterUserScramCredentialsInput,
    AlterUserScramCredentialsState, DeliveryStatus, Moment,
};

use super::super::{
    AlterUserScramCredentialsHandoff, AlterUserScramCredentialsHost,
    AlterUserScramCredentialsHostError,
};

impl AlterUserScramCredentialsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            if operation.rejected_submission.is_some() {
                return Err(AlterUserScramCredentialsHostError::SubmissionMismatch);
            }
            let operation_id = operation.operation_id;
            let state = operation.machine.state();
            let handoff = operation.handoff;
            match (state, handoff) {
                (AlterUserScramCredentialsState::Ready, _) => self.apply(
                    operation_id,
                    AlterUserScramCredentialsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    AlterUserScramCredentialsState::AwaitingDriver,
                    AlterUserScramCredentialsHandoff::Untouched,
                ) => self.apply(operation_id, AlterUserScramCredentialsInput::DriverRejected)?,
                (
                    AlterUserScramCredentialsState::AwaitingDriver,
                    AlterUserScramCredentialsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, AlterUserScramCredentialsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    AlterUserScramCredentialsState::Submitted,
                    AlterUserScramCredentialsHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AlterUserScramCredentialsState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(AlterUserScramCredentialsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        if self.operations[index].recovered_call.is_none() {
            let call = self.operations[index]
                .call
                .take()
                .ok_or(AlterUserScramCredentialsHostError::InvalidHandoff)?;
            match call.recover_after_driver_shutdown() {
                Ok(recovered) => self.operations[index].recovered_call = Some(recovered),
                Err(call) => {
                    self.operations[index].call = Some(call);
                    return Err(AlterUserScramCredentialsHostError::InvalidHandoff);
                }
            }
        }
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AlterUserScramCredentialsHostError::InvalidHandoff)?;
        if self.operations[index].matches_recovered(recovered) {
            Ok(())
        } else {
            Err(AlterUserScramCredentialsHostError::SubmissionMismatch)
        }
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AlterUserScramCredentialsHostError::InvalidHandoff)?;
        if !self.operations[index].matches_recovered(recovered) {
            return Err(AlterUserScramCredentialsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index].machine.apply(
            AlterUserScramCredentialsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let effect = transition
            .into_effect()
            .ok_or(AlterUserScramCredentialsHostError::MissingTerminal)?;
        let terminal = match effect {
            AlterUserScramCredentialsEffect::Complete {
                operation_id,
                terminal,
            } if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(AlterUserScramCredentialsHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(AlterUserScramCredentialsHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
