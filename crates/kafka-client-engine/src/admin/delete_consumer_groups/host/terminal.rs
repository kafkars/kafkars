//! Call polling, route release, publication, and reclamation.

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{DeleteConsumerGroupsHost, DeleteConsumerGroupsHostError, response::terminal_input};

impl DeleteConsumerGroupsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, DeleteConsumerGroupsHostError> {
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
                .ok_or(DeleteConsumerGroupsHostError::InvalidHandoff)?;
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
            Err(_error) => Err(DeleteConsumerGroupsHostError::CallCompletion),
        }
    }

    pub(super) fn settle_raw(&mut self, index: usize) -> Result<(), DeleteConsumerGroupsHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(DeleteConsumerGroupsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(DeleteConsumerGroupsHostError::MissingTerminal)?;
            let current = operation
                .machine
                .current_target()
                .ok_or(DeleteConsumerGroupsHostError::SubmissionMismatch)?;
            if !raw.matches_evidence(
                &operation.plan,
                current,
                operation.request_limit,
                operation.result_limit,
            ) {
                return Err(DeleteConsumerGroupsHostError::SubmissionMismatch);
            }
            terminal_input(raw)
        };
        let remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(DeleteConsumerGroupsHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        self.operations[index].remaining_result_bytes = remaining_result_bytes;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(DeleteConsumerGroupsHostError::MissingTerminal)?;
        raw.discard();
        let effect = transition
            .into_effect()
            .ok_or(DeleteConsumerGroupsHostError::MissingTerminal)?;
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), DeleteConsumerGroupsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(DeleteConsumerGroupsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DeleteConsumerGroupsHostError::MissingTerminal)?;
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
                Err(DeleteConsumerGroupsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DeleteConsumerGroupsHostError> {
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
            Err(error) => Err(DeleteConsumerGroupsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DeleteConsumerGroupsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(DeleteConsumerGroupsHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DeleteConsumerGroupsHostError::ByteAccounting)?;
        Ok(())
    }
}
