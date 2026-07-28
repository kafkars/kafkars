//! Call polling, route release, publication, reclamation, and recovery.

#[cfg(test)]
mod terminal_test;

use kafka_client_core::{AdminListOffsetsInput, AdminListOffsetsState, DeliveryStatus, Moment};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    AdminListOffsetsHandoff, AdminListOffsetsHost, AdminListOffsetsHostError,
    response::terminal_input,
};

impl AdminListOffsetsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, AdminListOffsetsHostError> {
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
                .ok_or(AdminListOffsetsHostError::InvalidHandoff)?;
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
            Err(_error) => Err(AdminListOffsetsHostError::CallCompletion),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), AdminListOffsetsHostError> {
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
                (AdminListOffsetsState::Ready, _) => self.apply(
                    operation_id,
                    AdminListOffsetsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (AdminListOffsetsState::AwaitingDriver, AdminListOffsetsHandoff::Untouched) => {
                    self.apply(operation_id, AdminListOffsetsInput::DriverRejected)?;
                }
                (AdminListOffsetsState::AwaitingDriver, AdminListOffsetsHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, AdminListOffsetsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (AdminListOffsetsState::Submitted, AdminListOffsetsHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AdminListOffsetsState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(AdminListOffsetsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), AdminListOffsetsHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AdminListOffsetsHostError::InvalidHandoff)?;
        let target = self.operations[index]
            .machine
            .current_target()
            .ok_or(AdminListOffsetsHostError::SubmissionMismatch)?;
        if !recovered.matches_correlation(target, self.operations[index].machine.read_isolation()) {
            return Err(AdminListOffsetsHostError::SubmissionMismatch);
        }
        Ok(())
    }

    fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), AdminListOffsetsHostError> {
        let target = self.operations[index]
            .machine
            .current_target()
            .ok_or(AdminListOffsetsHostError::SubmissionMismatch)?;
        if !self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AdminListOffsetsHostError::InvalidHandoff)?
            .matches_correlation(target, self.operations[index].machine.read_isolation())
        {
            return Err(AdminListOffsetsHostError::SubmissionMismatch);
        }
        let transition =
            self.operations[index]
                .machine
                .apply(AdminListOffsetsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let effect = transition
            .into_effect()
            .ok_or(AdminListOffsetsHostError::MissingTerminal)?;
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(AdminListOffsetsHostError::InvalidHandoff)?;
        recovered.seal();
        self.install_effect(index, effect)
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), AdminListOffsetsHostError> {
        let input = {
            let operation = self
                .operations
                .get(index)
                .ok_or(AdminListOffsetsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(AdminListOffsetsHostError::MissingTerminal)?;
            let current = operation
                .machine
                .current_target()
                .ok_or(AdminListOffsetsHostError::SubmissionMismatch)?;
            if !raw.matches_correlation(current, operation.machine.read_isolation()) {
                return Err(AdminListOffsetsHostError::SubmissionMismatch);
            }
            terminal_input(raw, current, operation.machine.read_isolation())
        };
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(AdminListOffsetsHostError::MissingTerminal)?;
        raw.discard();
        let effect = transition
            .into_effect()
            .ok_or(AdminListOffsetsHostError::MissingTerminal)?;
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), AdminListOffsetsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(AdminListOffsetsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(AdminListOffsetsHostError::MissingTerminal)?;
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
                Err(AdminListOffsetsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, AdminListOffsetsHostError> {
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
            Err(error) => Err(AdminListOffsetsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), AdminListOffsetsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(AdminListOffsetsHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(AdminListOffsetsHostError::ByteAccounting)?;
        Ok(())
    }
}
