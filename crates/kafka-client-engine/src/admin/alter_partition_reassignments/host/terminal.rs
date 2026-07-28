//! Polling, receipt release, publication, reclamation, and recovery.

use kafka_client_core::AlterPartitionReassignmentsEffect;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    AlterPartitionReassignmentsHost, AlterPartitionReassignmentsHostError, response::terminal_input,
};

impl AlterPartitionReassignmentsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, AlterPartitionReassignmentsHostError> {
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
            .ok_or(AlterPartitionReassignmentsHostError::InvalidHandoff)?
            .try_terminal();
        let Some(terminal) = terminal else {
            return Ok(false);
        };
        match terminal {
            Ok(terminal) => {
                drop(self.operations[index].call.take());
                self.operations[index].raw_terminal = Some(terminal);
                self.prepare_raw(index)?;
                if !self.operations[index].raw_terminal.as_ref().is_some_and(
                    crate::driver::AlterPartitionReassignmentsTerminal::controller_refresh_pending,
                ) {
                    self.settle_prepared_raw(index)?;
                }
                Ok(true)
            }
            Err(_error) => Err(AlterPartitionReassignmentsHostError::CallCompletion),
        }
    }

    pub(super) fn settle_raw(
        &mut self,
        index: usize,
    ) -> Result<(), AlterPartitionReassignmentsHostError> {
        self.prepare_raw(index)?;
        if self.operations[index].raw_terminal.as_ref().is_some_and(
            crate::driver::AlterPartitionReassignmentsTerminal::controller_refresh_pending,
        ) {
            return Err(AlterPartitionReassignmentsHostError::InvalidHandoff);
        }
        self.settle_prepared_raw(index)
    }

    pub(super) fn prepare_raw(
        &mut self,
        index: usize,
    ) -> Result<(), AlterPartitionReassignmentsHostError> {
        let operation = &mut self.operations[index];
        let raw = operation
            .raw_terminal
            .as_mut()
            .ok_or(AlterPartitionReassignmentsHostError::MissingTerminal)?;
        if raw.input_prepared() {
            return Ok(());
        }
        if !raw.matches_evidence(
            &operation.response_plan,
            operation.request_scratch_limit,
            operation.result_limit,
        ) {
            return Err(AlterPartitionReassignmentsHostError::SubmissionMismatch);
        }
        let input = terminal_input(raw);
        raw.prepare_input(input)
            .map_err(|_error| AlterPartitionReassignmentsHostError::InvalidHandoff)
    }

    fn settle_prepared_raw(
        &mut self,
        index: usize,
    ) -> Result<(), AlterPartitionReassignmentsHostError> {
        let input = {
            let operation = &mut self.operations[index];
            let raw = operation
                .raw_terminal
                .as_mut()
                .ok_or(AlterPartitionReassignmentsHostError::MissingTerminal)?;
            if !raw.matches_evidence(
                &operation.response_plan,
                operation.request_scratch_limit,
                operation.result_limit,
            ) {
                return Err(AlterPartitionReassignmentsHostError::SubmissionMismatch);
            }
            raw.take_input()
                .ok_or(AlterPartitionReassignmentsHostError::MissingTerminal)?
        };
        let transition = self.operations[index].machine.apply(input)?;
        self.operations[index]
            .raw_terminal
            .take()
            .ok_or(AlterPartitionReassignmentsHostError::MissingTerminal)?
            .discard();
        match transition.into_effect() {
            Some(AlterPartitionReassignmentsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(AlterPartitionReassignmentsHostError::MissingTerminal),
        }
    }

    pub(super) fn poll_one_controller_refresh(
        &mut self,
        driver: Option<&crate::driver::DriverOwner>,
    ) -> Result<bool, AlterPartitionReassignmentsHostError> {
        let Some(index) = self.operations.iter().position(|operation| {
            operation.raw_terminal.as_ref().is_some_and(
                crate::driver::AlterPartitionReassignmentsTerminal::controller_refresh_pending,
            )
        }) else {
            return Ok(false);
        };
        let driver = driver.ok_or(AlterPartitionReassignmentsHostError::InvalidHandoff)?;
        let ready = self.operations[index]
            .raw_terminal
            .as_mut()
            .ok_or(AlterPartitionReassignmentsHostError::MissingTerminal)?
            .poll_controller_refresh(driver);
        if !ready {
            return Ok(false);
        }
        self.settle_prepared_raw(index)?;
        Ok(true)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), AlterPartitionReassignmentsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(AlterPartitionReassignmentsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(AlterPartitionReassignmentsHostError::MissingTerminal)?;
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
                Err(AlterPartitionReassignmentsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, AlterPartitionReassignmentsHostError> {
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
                    .ok_or(AlterPartitionReassignmentsHostError::ByteAccounting)?;
                let (_id, bytes) = self.published_bytes.swap_remove(index);
                self.retained_bytes = self
                    .retained_bytes
                    .checked_sub(bytes)
                    .ok_or(AlterPartitionReassignmentsHostError::ByteAccounting)?;
                self.reclaim_pending = None;
                Ok(true)
            }
            Err(error) => Err(AlterPartitionReassignmentsHostError::Completion(error)),
        }
    }
}
