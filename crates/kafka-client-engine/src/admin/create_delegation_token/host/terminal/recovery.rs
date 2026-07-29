//! Exact accepted-call recovery after the unique driver has been destroyed.

use kafka_client_core::{
    CreateDelegationTokenEffect, CreateDelegationTokenInput, CreateDelegationTokenState,
    DeliveryStatus, Moment,
};

use super::super::{
    CreateDelegationTokenHandoff, CreateDelegationTokenHost, CreateDelegationTokenHostError,
};

impl CreateDelegationTokenHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), CreateDelegationTokenHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            let state = operation.machine.state();
            let handoff = operation.handoff;
            match (state, handoff) {
                (CreateDelegationTokenState::Ready, _) => self.apply(
                    operation_id,
                    CreateDelegationTokenInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    CreateDelegationTokenState::AwaitingDriver,
                    CreateDelegationTokenHandoff::Untouched,
                ) => {
                    self.apply(operation_id, CreateDelegationTokenInput::DriverRejected)?;
                }
                (
                    CreateDelegationTokenState::AwaitingDriver,
                    CreateDelegationTokenHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, CreateDelegationTokenInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    CreateDelegationTokenState::Submitted,
                    CreateDelegationTokenHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (CreateDelegationTokenState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(CreateDelegationTokenHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), CreateDelegationTokenHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(CreateDelegationTokenHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), CreateDelegationTokenHostError> {
        let transition =
            self.operations[index]
                .machine
                .apply(CreateDelegationTokenInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(CreateDelegationTokenEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(CreateDelegationTokenHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(CreateDelegationTokenHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
