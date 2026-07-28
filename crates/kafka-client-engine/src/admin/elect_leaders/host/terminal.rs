//! Polling, receipt release, publication, reclamation, and recovery.

use kafka_client_core::{
    DeliveryStatus, ElectLeadersEffect, ElectLeadersInput, ElectLeadersState, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    ElectLeadersHandoff, ElectLeadersHost, ElectLeadersHostError,
    recovered::{retain as retain_recovered, take as take_recovered},
    response::terminal_input,
};

impl ElectLeadersHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, ElectLeadersHostError> {
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.call.is_some())
        else {
            return Ok(false);
        };
        let terminal = self.operations[index]
            .call
            .as_mut()
            .ok_or(ElectLeadersHostError::InvalidHandoff)?
            .try_terminal();
        let Some(terminal) = terminal else {
            return Ok(false);
        };
        match terminal {
            Ok(terminal) => {
                drop(self.operations[index].call.take());
                self.operations[index].raw_terminal = Some(terminal);
                self.settle_raw(index)?;
                Ok(true)
            }
            Err(_error) => Err(ElectLeadersHostError::CallCompletion),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), ElectLeadersHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (ElectLeadersState::Ready, _) => self.apply(
                    operation_id,
                    ElectLeadersInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (ElectLeadersState::AwaitingDriver, ElectLeadersHandoff::Untouched) => {
                    self.apply(operation_id, ElectLeadersInput::DriverRejected)?;
                }
                (ElectLeadersState::AwaitingDriver, ElectLeadersHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, ElectLeadersInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (ElectLeadersState::Submitted, ElectLeadersHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (ElectLeadersState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(ElectLeadersHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), ElectLeadersHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            retain_recovered(
                &mut self.operations[index],
                call.recover_after_driver_shutdown(),
            );
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(ElectLeadersHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    fn settle_recovered_transport(&mut self, index: usize) -> Result<(), ElectLeadersHostError> {
        let transition =
            self.operations[index]
                .machine
                .apply(ElectLeadersInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(ElectLeadersEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(ElectLeadersHostError::MissingTerminal),
        };
        let recovered = take_recovered(&mut self.operations[index])
            .ok_or(ElectLeadersHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), ElectLeadersHostError> {
        let input = {
            let operation = &self.operations[index];
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(ElectLeadersHostError::MissingTerminal)?;
            terminal_input(raw, &operation.response_plan, operation.result_limit)
        };
        let transition = self.operations[index].machine.apply(input)?;
        self.operations[index]
            .raw_terminal
            .take()
            .ok_or(ElectLeadersHostError::MissingTerminal)?
            .discard();
        match transition.into_effect() {
            Some(ElectLeadersEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(ElectLeadersHostError::MissingTerminal),
        }
    }

    pub(super) fn publish_terminal(&mut self, index: usize) -> Result<(), ElectLeadersHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(ElectLeadersHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(ElectLeadersHostError::MissingTerminal)?;
        let completion_id = self.operations[index].completion_id;
        match self.completions.publish(completion_id, terminal) {
            Ok(()) => {
                let operation = self.operations.remove(index);
                self.published_bytes
                    .push((operation.completion_id, operation.retained_bytes));
                Ok(())
            }
            Err((error, terminal)) => {
                self.operations[index].terminal = Some(terminal);
                Err(ElectLeadersHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, ElectLeadersHostError> {
        let completion_id = if let Some(id) = self.reclaim_pending {
            id
        } else {
            let Some(id) = self.completions.next_reclaim()? else {
                return Ok(false);
            };
            self.reclaim_pending = Some(id);
            id
        };
        match self.completions.finish_reclaim(completion_id) {
            Ok(ReclaimStatus::Retry) => Ok(false),
            Ok(ReclaimStatus::Reclaimed) | Err(CompletionRegistryError::GenerationExhausted) => {
                let index = self
                    .published_bytes
                    .iter()
                    .position(|(id, _bytes)| *id == completion_id)
                    .ok_or(ElectLeadersHostError::ByteAccounting)?;
                let (_id, bytes) = self.published_bytes.swap_remove(index);
                self.retained_bytes = self
                    .retained_bytes
                    .checked_sub(bytes)
                    .ok_or(ElectLeadersHostError::ByteAccounting)?;
                self.reclaim_pending = None;
                Ok(true)
            }
            Err(error) => Err(ElectLeadersHostError::Completion(error)),
        }
    }
}
