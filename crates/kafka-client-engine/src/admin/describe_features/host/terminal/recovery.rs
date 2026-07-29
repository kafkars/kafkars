//! Exact accepted-call recovery after the unique driver has been destroyed.

use kafka_client_core::{
    DeliveryStatus, DescribeFeaturesEffect, DescribeFeaturesInput, DescribeFeaturesState, Moment,
};

use super::super::{DescribeFeaturesHandoff, DescribeFeaturesHost, DescribeFeaturesHostError};

impl DescribeFeaturesHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DescribeFeaturesHostError> {
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
                (DescribeFeaturesState::Ready, _) => self.apply(
                    operation_id,
                    DescribeFeaturesInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (DescribeFeaturesState::AwaitingDriver, DescribeFeaturesHandoff::Untouched) => {
                    self.apply(operation_id, DescribeFeaturesInput::DriverRejected)?;
                }
                (DescribeFeaturesState::AwaitingDriver, DescribeFeaturesHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, DescribeFeaturesInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (DescribeFeaturesState::Submitted, DescribeFeaturesHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (DescribeFeaturesState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(DescribeFeaturesHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), DescribeFeaturesHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(DescribeFeaturesHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeFeaturesHostError> {
        let transition =
            self.operations[index]
                .machine
                .apply(DescribeFeaturesInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let effect = transition
            .into_effect()
            .ok_or(DescribeFeaturesHostError::MissingTerminal)?;
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DescribeFeaturesHostError::InvalidHandoff)?;
        recovered.seal();
        match effect {
            DescribeFeaturesEffect::Complete {
                operation_id,
                terminal,
            } if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(DescribeFeaturesHostError::MissingTerminal),
        }
    }
}
