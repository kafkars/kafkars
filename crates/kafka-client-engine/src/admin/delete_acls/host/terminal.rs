//! Call polling, stable translation, publication, reclamation, and recovery.

mod recovery;

#[cfg(test)]
mod test_support;

use kafka_client_core::DeleteAclsEffect;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{DeleteAclsHost, DeleteAclsHostError, response::terminal_input};

impl DeleteAclsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, DeleteAclsHostError> {
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
                .ok_or(DeleteAclsHostError::InvalidHandoff)?;
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
            Err(_error) => Err(DeleteAclsHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), DeleteAclsHostError> {
        let (expected_filters, request_limit) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(DeleteAclsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(DeleteAclsHostError::MissingTerminal)?;
            if !operation.matches_raw(raw) {
                return Err(DeleteAclsHostError::SubmissionMismatch);
            }
            (
                raw.plan().required_filter_result_capacity(),
                raw.request_limit(),
            )
        };
        let prepared_results = self.operations[index]
            .prepared_results
            .take()
            .ok_or(DeleteAclsHostError::MissingResultStorage)?;
        let mut matching_counts = core::mem::take(&mut self.operations[index].matching_counts);
        let mut prepared_outcomes = self.operations[index]
            .prepared_outcomes
            .take()
            .ok_or(DeleteAclsHostError::MissingOutcomeStorage)?;
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(DeleteAclsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(DeleteAclsHostError::MissingTerminal)?;
            terminal_input(
                raw,
                expected_filters,
                prepared_results,
                &mut matching_counts,
                &mut prepared_outcomes,
                operation.prepared_outcome_bytes,
                operation.prepared_core_result_bytes,
                request_limit,
            )
        };
        self.operations[index].matching_counts = matching_counts;
        self.operations[index].prepared_outcomes = Some(prepared_outcomes);
        self.operations[index].remaining_response_bytes = self.operations[index]
            .remaining_response_bytes
            .checked_sub(retained_bytes)
            .ok_or(DeleteAclsHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(DeleteAclsHostError::MissingTerminal)?;
        raw.discard();
        match transition.into_effect() {
            Some(DeleteAclsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(DeleteAclsHostError::MissingTerminal),
        }
    }

    pub(super) fn publish_terminal(&mut self, index: usize) -> Result<(), DeleteAclsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(DeleteAclsHostError::InvalidHandoff);
        }
        if self.operations[index].outcome.is_none() {
            let terminal = self.operations[index]
                .terminal
                .take()
                .ok_or(DeleteAclsHostError::MissingTerminal)?;
            let prepared = self.operations[index]
                .prepared_outcomes
                .take()
                .ok_or(DeleteAclsHostError::MissingOutcomeStorage)?;
            match super::super::translate_terminal_into(terminal, prepared) {
                Ok(outcome) => self.operations[index].outcome = Some(outcome),
                Err(failure) => {
                    let (error, terminal, prepared) = failure.into_parts();
                    self.operations[index].terminal = Some(terminal);
                    self.operations[index].prepared_outcomes = Some(prepared);
                    return Err(DeleteAclsHostError::Translation(error));
                }
            }
        }
        let outcome = self.operations[index]
            .outcome
            .take()
            .ok_or(DeleteAclsHostError::MissingTerminal)?;
        let completion_id = self.operations[index].completion_id;
        match self.completions.publish(completion_id, outcome) {
            Ok(()) => {
                let operation = self.operations.remove(index);
                self.published_bytes
                    .push((operation.completion_id, operation.retained_bytes));
                Ok(())
            }
            Err((error, outcome)) => {
                self.operations[index].outcome = Some(outcome);
                Err(DeleteAclsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DeleteAclsHostError> {
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
            Err(error) => Err(DeleteAclsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DeleteAclsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(DeleteAclsHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DeleteAclsHostError::ByteAccounting)?;
        Ok(())
    }
}
