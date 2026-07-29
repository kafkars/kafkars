//! Exact accepted-call and transaction-identity recovery after driver destruction.

use kafka_client_core::{
    AbortPartitionTransactionEffect, AbortPartitionTransactionInput,
    AbortPartitionTransactionState, DeliveryStatus, Moment,
};

use super::super::{
    AbortPartitionTransactionHandoff, AbortPartitionTransactionHost,
    AbortPartitionTransactionHostError,
};

impl AbortPartitionTransactionHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), AbortPartitionTransactionHostError> {
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
                (AbortPartitionTransactionState::Ready, _) => self.apply(
                    operation_id,
                    AbortPartitionTransactionInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    AbortPartitionTransactionState::AwaitingDriver,
                    AbortPartitionTransactionHandoff::Untouched,
                ) => self.apply(operation_id, AbortPartitionTransactionInput::DriverRejected)?,
                (
                    AbortPartitionTransactionState::AwaitingDriver,
                    AbortPartitionTransactionHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, AbortPartitionTransactionInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    AbortPartitionTransactionState::Submitted,
                    AbortPartitionTransactionHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AbortPartitionTransactionState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(AbortPartitionTransactionHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), AbortPartitionTransactionHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AbortPartitionTransactionHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), AbortPartitionTransactionHostError> {
        let transition = self.operations[index].machine.apply(
            AbortPartitionTransactionInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let effect = transition
            .into_effect()
            .ok_or(AbortPartitionTransactionHostError::MissingTerminal)?;
        let terminal = match effect {
            AbortPartitionTransactionEffect::Complete {
                operation_id,
                terminal,
            } if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(AbortPartitionTransactionHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(AbortPartitionTransactionHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
