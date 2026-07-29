//! Exact accepted-call and secret-correlation recovery after driver destruction.

use kafka_client_core::{
    DeliveryStatus, ExpireDelegationTokenEffect, ExpireDelegationTokenInput,
    ExpireDelegationTokenState, Moment,
};

use super::super::{
    ExpireDelegationTokenHandoff, ExpireDelegationTokenHost, ExpireDelegationTokenHostError,
};

impl ExpireDelegationTokenHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ExpireDelegationTokenHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (ExpireDelegationTokenState::Ready, _) => self.apply(
                    operation_id,
                    ExpireDelegationTokenInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    ExpireDelegationTokenState::AwaitingDriver,
                    ExpireDelegationTokenHandoff::Untouched,
                ) => self.apply(operation_id, ExpireDelegationTokenInput::DriverRejected)?,
                (
                    ExpireDelegationTokenState::AwaitingDriver,
                    ExpireDelegationTokenHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, ExpireDelegationTokenInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    ExpireDelegationTokenState::Submitted,
                    ExpireDelegationTokenHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (ExpireDelegationTokenState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(ExpireDelegationTokenHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), ExpireDelegationTokenHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(ExpireDelegationTokenHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), ExpireDelegationTokenHostError> {
        let transition =
            self.operations[index]
                .machine
                .apply(ExpireDelegationTokenInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(ExpireDelegationTokenEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(ExpireDelegationTokenHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(ExpireDelegationTokenHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
