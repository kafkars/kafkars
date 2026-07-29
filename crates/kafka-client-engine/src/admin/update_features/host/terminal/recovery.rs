//! Exact accepted-call recovery after the unique driver has been destroyed.

use kafka_client_core::{
    DeliveryStatus, Moment, UpdateFeaturesEffect, UpdateFeaturesInput, UpdateFeaturesState,
};

use super::super::{UpdateFeaturesHandoff, UpdateFeaturesHost, UpdateFeaturesHostError};

impl UpdateFeaturesHost {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), UpdateFeaturesHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (UpdateFeaturesState::Ready, _) => self.apply(
                    operation_id,
                    UpdateFeaturesInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (UpdateFeaturesState::AwaitingDriver, UpdateFeaturesHandoff::Untouched) => {
                    self.apply(operation_id, UpdateFeaturesInput::DriverRejected)?;
                }
                (UpdateFeaturesState::AwaitingDriver, UpdateFeaturesHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, UpdateFeaturesInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (UpdateFeaturesState::Submitted, UpdateFeaturesHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (UpdateFeaturesState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(UpdateFeaturesHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), UpdateFeaturesHostError> {
        if self.operations[index].recovered_call.is_none()
            && let Some(call) = self.operations[index].call.take()
        {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let operation = &self.operations[index];
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(UpdateFeaturesHostError::InvalidHandoff)?;
        if !recovered.matches_evidence(&operation.response_plan, operation.remaining_result_bytes) {
            return Err(UpdateFeaturesHostError::SubmissionMismatch);
        }
        Ok(())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), UpdateFeaturesHostError> {
        {
            let operation = &self.operations[index];
            let recovered = operation
                .recovered_call
                .as_ref()
                .ok_or(UpdateFeaturesHostError::InvalidHandoff)?;
            if !recovered
                .matches_evidence(&operation.response_plan, operation.remaining_result_bytes)
            {
                return Err(UpdateFeaturesHostError::SubmissionMismatch);
            }
        }
        let transition =
            self.operations[index]
                .machine
                .apply(UpdateFeaturesInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(UpdateFeaturesEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(UpdateFeaturesHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(UpdateFeaturesHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
