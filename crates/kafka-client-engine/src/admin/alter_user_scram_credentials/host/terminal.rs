//! Call polling, route release, publication, reclamation, and recovery.

mod recovery;
#[cfg(test)]
mod recovery_test;

#[cfg(test)]
mod test_support;

use kafka_client_core::AlterUserScramCredentialsEffect;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    AlterUserScramCredentialsHost, AlterUserScramCredentialsHostError, response::terminal_input,
};

impl AlterUserScramCredentialsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, AlterUserScramCredentialsHostError> {
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
                .ok_or(AlterUserScramCredentialsHostError::InvalidHandoff)?;
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
            Err(_error) => Err(AlterUserScramCredentialsHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), AlterUserScramCredentialsHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(AlterUserScramCredentialsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(AlterUserScramCredentialsHostError::MissingTerminal)?;
            if !operation.matches_raw(raw) {
                return Err(AlterUserScramCredentialsHostError::SubmissionMismatch);
            }
            terminal_input(raw, operation.bounds.result_limit)
        };
        self.operations[index]
            .bounds
            .result_limit
            .checked_sub(retained_bytes)
            .ok_or(AlterUserScramCredentialsHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let terminal = match transition.into_effect() {
            Some(AlterUserScramCredentialsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(AlterUserScramCredentialsHostError::MissingTerminal),
        };
        self.operations[index]
            .raw_terminal
            .take()
            .ok_or(AlterUserScramCredentialsHostError::MissingTerminal)?
            .discard();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(AlterUserScramCredentialsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(AlterUserScramCredentialsHostError::MissingTerminal)?;
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
                Err(AlterUserScramCredentialsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, AlterUserScramCredentialsHostError> {
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
            Err(error) => Err(AlterUserScramCredentialsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(AlterUserScramCredentialsHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(AlterUserScramCredentialsHostError::ByteAccounting)?;
        Ok(())
    }
}
