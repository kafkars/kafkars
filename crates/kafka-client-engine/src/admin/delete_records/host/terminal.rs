//! Call polling, route release, publication, reclamation, and recovery.

#[cfg(test)]
mod test_support;

use kafka_client_core::{DeleteRecordsInput, DeleteRecordsState, DeliveryStatus, Moment};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    DeleteRecordsHandoff, DeleteRecordsHost, DeleteRecordsHostError, response::terminal_input,
};

impl DeleteRecordsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, DeleteRecordsHostError> {
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.call.is_some())
        else {
            return Ok(false);
        };
        let terminal = {
            let call = self.operations[index]
                .call
                .as_mut()
                .ok_or(DeleteRecordsHostError::InvalidHandoff)?;
            call.try_terminal()
        };
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
            Err(_error) => Err(DeleteRecordsHostError::CallCompletion),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), DeleteRecordsHostError> {
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
                (DeleteRecordsState::Ready, _) => self.apply(
                    operation_id,
                    DeleteRecordsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (DeleteRecordsState::AwaitingDriver, DeleteRecordsHandoff::Untouched) => {
                    self.apply(operation_id, DeleteRecordsInput::DriverRejected)?;
                }
                (DeleteRecordsState::AwaitingDriver, DeleteRecordsHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, DeleteRecordsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (DeleteRecordsState::Submitted, DeleteRecordsHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (DeleteRecordsState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(DeleteRecordsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), DeleteRecordsHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(DeleteRecordsHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    fn settle_recovered_transport(&mut self, index: usize) -> Result<(), DeleteRecordsHostError> {
        let transition =
            self.operations[index]
                .machine
                .apply(DeleteRecordsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let effect = transition
            .into_effect()
            .ok_or(DeleteRecordsHostError::MissingTerminal)?;
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DeleteRecordsHostError::InvalidHandoff)?;
        recovered.seal();
        self.install_effect(index, effect)
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), DeleteRecordsHostError> {
        let input = {
            let operation = self
                .operations
                .get(index)
                .ok_or(DeleteRecordsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(DeleteRecordsHostError::MissingTerminal)?;
            let current = operation
                .machine
                .current_target()
                .ok_or(DeleteRecordsHostError::SubmissionMismatch)?;
            terminal_input(raw, current)
        };
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(DeleteRecordsHostError::MissingTerminal)?;
        raw.discard();
        let effect = transition
            .into_effect()
            .ok_or(DeleteRecordsHostError::MissingTerminal)?;
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(&mut self, index: usize) -> Result<(), DeleteRecordsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(DeleteRecordsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DeleteRecordsHostError::MissingTerminal)?;
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
                Err(DeleteRecordsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DeleteRecordsHostError> {
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
                self.release_published_bytes(completion_id)?;
                self.reclaim_pending = None;
                Ok(true)
            }
            Err(error) => Err(DeleteRecordsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DeleteRecordsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(DeleteRecordsHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DeleteRecordsHostError::ByteAccounting)?;
        Ok(())
    }
}
