//! Exact accepted-call recovery after the unique driver has been destroyed.

use kafka_client_core::{
    DeliveryStatus, ListClientMetricsResourcesEffect, ListClientMetricsResourcesInput,
    ListClientMetricsResourcesState, Moment,
};

use super::super::{
    ListClientMetricsResourcesHandoff, ListClientMetricsResourcesHost,
    ListClientMetricsResourcesHostError,
};

impl ListClientMetricsResourcesHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ListClientMetricsResourcesHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (ListClientMetricsResourcesState::Ready, _) => self.apply(
                    operation_id,
                    ListClientMetricsResourcesInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    ListClientMetricsResourcesState::AwaitingDriver,
                    ListClientMetricsResourcesHandoff::Untouched,
                ) => self.apply(
                    operation_id,
                    ListClientMetricsResourcesInput::DriverRejected,
                )?,
                (
                    ListClientMetricsResourcesState::AwaitingDriver,
                    ListClientMetricsResourcesHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(
                        operation_id,
                        ListClientMetricsResourcesInput::DriverAccepted,
                    )?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    ListClientMetricsResourcesState::Submitted,
                    ListClientMetricsResourcesHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (ListClientMetricsResourcesState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(ListClientMetricsResourcesHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), ListClientMetricsResourcesHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(ListClientMetricsResourcesHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), ListClientMetricsResourcesHostError> {
        let transition = self.operations[index].machine.apply(
            ListClientMetricsResourcesInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let terminal = match transition.into_effect() {
            Some(ListClientMetricsResourcesEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(ListClientMetricsResourcesHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(ListClientMetricsResourcesHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
