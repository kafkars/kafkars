//! Exact accepted-call and broker-correlation recovery after driver destruction.

use kafka_client_core::{
    DeliveryStatus, Moment, UnregisterBrokerEffect, UnregisterBrokerInput, UnregisterBrokerState,
};

use super::super::{UnregisterBrokerHandoff, UnregisterBrokerHost, UnregisterBrokerHostError};

impl UnregisterBrokerHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), UnregisterBrokerHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (UnregisterBrokerState::Ready, _) => self.apply(
                    operation_id,
                    UnregisterBrokerInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (UnregisterBrokerState::AwaitingDriver, UnregisterBrokerHandoff::Untouched) => {
                    self.apply(operation_id, UnregisterBrokerInput::DriverRejected)?;
                }
                (UnregisterBrokerState::AwaitingDriver, UnregisterBrokerHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, UnregisterBrokerInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (UnregisterBrokerState::Submitted, UnregisterBrokerHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (UnregisterBrokerState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(UnregisterBrokerHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), UnregisterBrokerHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(UnregisterBrokerHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), UnregisterBrokerHostError> {
        let transition =
            self.operations[index]
                .machine
                .apply(UnregisterBrokerInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(UnregisterBrokerEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(UnregisterBrokerHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(UnregisterBrokerHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
