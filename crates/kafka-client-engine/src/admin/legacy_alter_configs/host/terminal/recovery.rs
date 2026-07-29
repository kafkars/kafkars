//! Exact accepted-call and route-local configuration recovery after driver destruction.

use kafka_client_core::{
    DeliveryStatus, LegacyAlterConfigsEffect, LegacyAlterConfigsInput, LegacyAlterConfigsState,
    Moment,
};

use super::super::{
    LegacyAlterConfigsHandoff, LegacyAlterConfigsHost, LegacyAlterConfigsHostError,
};

impl LegacyAlterConfigsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (LegacyAlterConfigsState::Ready, _) => self.apply(
                    operation_id,
                    LegacyAlterConfigsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (LegacyAlterConfigsState::AwaitingDriver, LegacyAlterConfigsHandoff::Untouched) => {
                    self.apply(operation_id, LegacyAlterConfigsInput::DriverRejected)?;
                }
                (LegacyAlterConfigsState::AwaitingDriver, LegacyAlterConfigsHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, LegacyAlterConfigsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (LegacyAlterConfigsState::Submitted, LegacyAlterConfigsHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (LegacyAlterConfigsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(LegacyAlterConfigsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), LegacyAlterConfigsHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(LegacyAlterConfigsHostError::InvalidHandoff)?;
        let route = self.operations[index]
            .route
            .ok_or(LegacyAlterConfigsHostError::SubmissionMismatch)?;
        let plan = self.operations[index]
            .plan
            .as_ref()
            .ok_or(LegacyAlterConfigsHostError::SubmissionMismatch)?;
        if !recovered.matches_correlation(route, plan) {
            return Err(LegacyAlterConfigsHostError::SubmissionMismatch);
        }
        Ok(())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(LegacyAlterConfigsHostError::InvalidHandoff)?;
        let route = self.operations[index]
            .route
            .ok_or(LegacyAlterConfigsHostError::SubmissionMismatch)?;
        let plan = self.operations[index]
            .plan
            .as_ref()
            .ok_or(LegacyAlterConfigsHostError::SubmissionMismatch)?;
        if !recovered.matches_correlation(route, plan) {
            return Err(LegacyAlterConfigsHostError::SubmissionMismatch);
        }
        let transition =
            self.operations[index]
                .machine
                .apply(LegacyAlterConfigsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(LegacyAlterConfigsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(LegacyAlterConfigsHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(LegacyAlterConfigsHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
