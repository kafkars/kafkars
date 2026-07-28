//! Call polling, route release, publication, reclamation, and recovery.

mod recovery;

#[cfg(test)]
mod test_support;

use kafka_client_core::{CreateAclsEffect, CreateAclsInput};

use crate::{
    admin::create_acls::outcome::translate_terminal_into,
    completion::{CompletionRegistryError, ReclaimStatus},
};

use super::{CreateAclsHost, CreateAclsHostError, response::terminal_input};

impl CreateAclsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, CreateAclsHostError> {
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
                .ok_or(CreateAclsHostError::InvalidHandoff)?;
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
            Err(_error) => Err(CreateAclsHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), CreateAclsHostError> {
        {
            let operation = self
                .operations
                .get(index)
                .ok_or(CreateAclsHostError::UnknownOperation)?;
            let plan = operation
                .machine
                .plan()
                .ok_or(CreateAclsHostError::SubmissionMismatch)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(CreateAclsHostError::MissingTerminal)?;
            if !raw.matches_evidence(plan, operation.request_limit, operation.result_limit) {
                return Err(CreateAclsHostError::SubmissionMismatch);
            }
        }
        let mut prepared_results = self.operations[index]
            .prepared_results
            .take()
            .ok_or(CreateAclsHostError::MissingResultStorage)?;
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(CreateAclsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(CreateAclsHostError::MissingTerminal)?;
            terminal_input(raw, &mut prepared_results)
        };
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(CreateAclsHostError::ByteAccounting)?;
        if !matches!(&input, CreateAclsInput::BrokerResponded { .. }) {
            self.operations[index].prepared_results = Some(prepared_results);
        }
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(CreateAclsHostError::MissingTerminal)?;
        raw.discard();
        match transition.into_effect() {
            Some(CreateAclsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(CreateAclsHostError::MissingTerminal),
        }
    }

    pub(super) fn publish_terminal(&mut self, index: usize) -> Result<(), CreateAclsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(CreateAclsHostError::InvalidHandoff);
        }
        if self.operations[index].outcome.is_none() {
            let terminal = self.operations[index]
                .terminal
                .take()
                .ok_or(CreateAclsHostError::MissingTerminal)?;
            let prepared_outcomes = self.operations[index]
                .prepared_outcomes
                .take()
                .ok_or(CreateAclsHostError::MissingOutcomeStorage)?;
            match translate_terminal_into(terminal, prepared_outcomes) {
                Ok(outcome) => self.operations[index].outcome = Some(outcome),
                Err(failure) => {
                    let error = failure.error();
                    let (_retained_error, terminal, prepared_outcomes) = failure.into_parts();
                    self.operations[index].terminal = Some(terminal);
                    self.operations[index].prepared_outcomes = Some(prepared_outcomes);
                    return Err(CreateAclsHostError::Translation(error));
                }
            }
        }
        let outcome = self.operations[index]
            .outcome
            .take()
            .ok_or(CreateAclsHostError::MissingTerminal)?;
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
                Err(CreateAclsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, CreateAclsHostError> {
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
            Err(error) => Err(CreateAclsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), CreateAclsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(CreateAclsHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(CreateAclsHostError::ByteAccounting)?;
        Ok(())
    }
}
