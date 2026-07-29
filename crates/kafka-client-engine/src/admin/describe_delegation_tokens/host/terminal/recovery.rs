//! Exact accepted-call and owner-selection recovery after driver destruction.

use kafka_client_core::{
    DeliveryStatus, DescribeDelegationTokensEffect, DescribeDelegationTokensInput,
    DescribeDelegationTokensState, Moment,
};

use super::super::{
    DescribeDelegationTokensHandoff, DescribeDelegationTokensHost,
    DescribeDelegationTokensHostError,
};

impl DescribeDelegationTokensHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DescribeDelegationTokensHostError> {
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
                (DescribeDelegationTokensState::Ready, _) => self.apply(
                    operation_id,
                    DescribeDelegationTokensInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    DescribeDelegationTokensState::AwaitingDriver,
                    DescribeDelegationTokensHandoff::Untouched,
                ) => self.apply(operation_id, DescribeDelegationTokensInput::DriverRejected)?,
                (
                    DescribeDelegationTokensState::AwaitingDriver,
                    DescribeDelegationTokensHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, DescribeDelegationTokensInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    DescribeDelegationTokensState::Submitted,
                    DescribeDelegationTokensHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (DescribeDelegationTokensState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(DescribeDelegationTokensHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeDelegationTokensHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if self.operations[index].call.is_none()
            || self.operations[index].correlation_plan.is_none()
        {
            return Err(DescribeDelegationTokensHostError::InvalidHandoff);
        }
        let call = self.operations[index]
            .call
            .take()
            .ok_or(DescribeDelegationTokensHostError::InvalidHandoff)?;
        let plan = self.operations[index]
            .correlation_plan
            .take()
            .ok_or(DescribeDelegationTokensHostError::InvalidHandoff)?;
        self.operations[index].recovered_call = call.recover_after_driver_shutdown(plan);
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(DescribeDelegationTokensHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeDelegationTokensHostError> {
        let transition = self.operations[index].machine.apply(
            DescribeDelegationTokensInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let effect = transition
            .into_effect()
            .ok_or(DescribeDelegationTokensHostError::MissingTerminal)?;
        let terminal = match effect {
            DescribeDelegationTokensEffect::Complete {
                operation_id,
                terminal,
            } if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(DescribeDelegationTokensHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DescribeDelegationTokensHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
