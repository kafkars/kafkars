//! Call polling, route release, publication, and reclamation.

mod recovery;
#[cfg(test)]
mod recovery_test;
#[cfg(test)]
mod test_support;

use kafka_client_core::ListPartitionReassignmentsEffect;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    ListPartitionReassignmentsHost, ListPartitionReassignmentsHostError, response::terminal_input,
};

impl ListPartitionReassignmentsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, ListPartitionReassignmentsHostError> {
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
                .ok_or(ListPartitionReassignmentsHostError::InvalidHandoff)?;
            call.try_terminal()
        };
        let Some(terminal) = terminal else {
            return Ok(false);
        };
        match terminal {
            Ok(terminal) => {
                drop(self.operations[index].call.take());
                self.operations[index].raw_terminal = Some(terminal);
                self.prepare_raw(index)?;
                if !self.operations[index]
                    .raw_terminal
                    .as_ref()
                    .is_some_and(crate::driver::ListPartitionReassignmentsRawTerminal::controller_refresh_pending)
                {
                    self.settle_prepared_raw(index)?;
                }
                Ok(true)
            }
            Err(_error) => Err(ListPartitionReassignmentsHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), ListPartitionReassignmentsHostError> {
        self.prepare_raw(index)?;
        if self.operations[index].raw_terminal.as_ref().is_some_and(
            crate::driver::ListPartitionReassignmentsRawTerminal::controller_refresh_pending,
        ) {
            return Err(ListPartitionReassignmentsHostError::InvalidHandoff);
        }
        self.settle_prepared_raw(index)
    }

    fn prepare_raw(&mut self, index: usize) -> Result<(), ListPartitionReassignmentsHostError> {
        let operation = &mut self.operations[index];
        let raw = operation
            .raw_terminal
            .as_mut()
            .ok_or(ListPartitionReassignmentsHostError::MissingTerminal)?;
        if raw.input_prepared() {
            return Ok(());
        }
        if !raw.matches(&operation.plan, operation.result_limit) {
            return Err(ListPartitionReassignmentsHostError::SubmissionMismatch);
        }
        let input = terminal_input(raw, &operation.plan, operation.result_limit);
        raw.prepare_input(input)
            .map_err(|_error| ListPartitionReassignmentsHostError::InvalidHandoff)
    }

    fn settle_prepared_raw(
        &mut self,
        index: usize,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        let input = {
            let operation = self
                .operations
                .get_mut(index)
                .ok_or(ListPartitionReassignmentsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_mut()
                .ok_or(ListPartitionReassignmentsHostError::MissingTerminal)?;
            if !raw.matches(&operation.plan, operation.result_limit) {
                return Err(ListPartitionReassignmentsHostError::SubmissionMismatch);
            }
            raw.take_input()
                .ok_or(ListPartitionReassignmentsHostError::MissingTerminal)?
        };
        let transition = self.operations[index].machine.apply(input)?;
        let terminal = match transition.into_effect() {
            Some(ListPartitionReassignmentsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(ListPartitionReassignmentsHostError::MissingTerminal),
        };
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(ListPartitionReassignmentsHostError::MissingTerminal)?;
        raw.discard();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }

    pub(super) fn poll_one_controller_refresh(
        &mut self,
        driver: Option<&crate::driver::DriverOwner>,
    ) -> Result<bool, ListPartitionReassignmentsHostError> {
        let Some(index) = self.operations.iter().position(|operation| {
            operation.raw_terminal.as_ref().is_some_and(
                crate::driver::ListPartitionReassignmentsRawTerminal::controller_refresh_pending,
            )
        }) else {
            return Ok(false);
        };
        let driver = driver.ok_or(ListPartitionReassignmentsHostError::InvalidHandoff)?;
        let ready = self.operations[index]
            .raw_terminal
            .as_mut()
            .ok_or(ListPartitionReassignmentsHostError::MissingTerminal)?
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
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(ListPartitionReassignmentsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(ListPartitionReassignmentsHostError::MissingTerminal)?;
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
                Err(ListPartitionReassignmentsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, ListPartitionReassignmentsHostError> {
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
            Err(error) => Err(ListPartitionReassignmentsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(ListPartitionReassignmentsHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(ListPartitionReassignmentsHostError::ByteAccounting)?;
        Ok(())
    }
}
