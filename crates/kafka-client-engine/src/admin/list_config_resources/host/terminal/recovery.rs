//! Exact accepted-call recovery after the unique driver has been destroyed.

use kafka_client_core::{
    DeliveryStatus, ListConfigResourcesEffect, ListConfigResourcesInput, ListConfigResourcesState,
    Moment,
};

use super::super::{
    ListConfigResourcesHandoff, ListConfigResourcesHost, ListConfigResourcesHostError,
};

impl ListConfigResourcesHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ListConfigResourcesHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (ListConfigResourcesState::Ready, _) => self.apply(
                    operation_id,
                    ListConfigResourcesInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    ListConfigResourcesState::AwaitingDriver,
                    ListConfigResourcesHandoff::Untouched,
                ) => self.apply(operation_id, ListConfigResourcesInput::DriverRejected)?,
                (
                    ListConfigResourcesState::AwaitingDriver,
                    ListConfigResourcesHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, ListConfigResourcesInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (ListConfigResourcesState::Submitted, ListConfigResourcesHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (ListConfigResourcesState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(ListConfigResourcesHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), ListConfigResourcesHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(ListConfigResourcesHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), ListConfigResourcesHostError> {
        let transition =
            self.operations[index]
                .machine
                .apply(ListConfigResourcesInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(ListConfigResourcesEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(ListConfigResourcesHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(ListConfigResourcesHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
