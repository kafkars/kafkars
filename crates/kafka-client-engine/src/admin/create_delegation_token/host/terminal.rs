//! Call polling, route release, publication, reclamation, and recovery.

mod recovery;

#[cfg(test)]
mod recovery_test;
#[cfg(test)]
mod test_support;

use kafka_client_core::CreateDelegationTokenEffect;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    CreateDelegationTokenHost, CreateDelegationTokenHostError, CreateDelegationTokenOperation,
    response::terminal_input,
};

impl CreateDelegationTokenOperation {
    fn poll_call(&mut self) -> Result<bool, CreateDelegationTokenHostError> {
        let terminal = self
            .call
            .as_mut()
            .ok_or(CreateDelegationTokenHostError::InvalidHandoff)?
            .try_terminal();
        let Some(terminal) = terminal else {
            return Ok(false);
        };
        match terminal {
            Ok(terminal) => {
                drop(self.call.take());
                self.raw_terminal = Some(terminal);
                Ok(true)
            }
            Err(_error) => Err(CreateDelegationTokenHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self) -> Result<(), CreateDelegationTokenHostError> {
        let raw = self
            .raw_terminal
            .as_ref()
            .ok_or(CreateDelegationTokenHostError::MissingTerminal)?;
        let (input, retained_bytes) = terminal_input(raw, self.remaining_result_bytes);
        self.remaining_result_bytes = self
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(CreateDelegationTokenHostError::ByteAccounting)?;
        let transition = self.machine.apply(input)?;
        let raw = self
            .raw_terminal
            .take()
            .ok_or(CreateDelegationTokenHostError::MissingTerminal)?;
        raw.discard();
        match transition.into_effect() {
            Some(CreateDelegationTokenEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operation_id => {
                self.terminal = Some(terminal);
                Ok(())
            }
            _ => Err(CreateDelegationTokenHostError::MissingTerminal),
        }
    }
}

impl CreateDelegationTokenHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, CreateDelegationTokenHostError> {
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.call.is_some())
        else {
            return Ok(false);
        };
        if !self.operations[index].poll_call()? {
            return Ok(false);
        }
        self.settle_raw(index)?;
        Ok(true)
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), CreateDelegationTokenHostError> {
        let operation = self
            .operations
            .get_mut(index)
            .ok_or(CreateDelegationTokenHostError::UnknownOperation)?;
        operation.settle_raw()?;
        self.publish_terminal(index)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), CreateDelegationTokenHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(CreateDelegationTokenHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(CreateDelegationTokenHostError::MissingTerminal)?;
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
                Err(CreateDelegationTokenHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, CreateDelegationTokenHostError> {
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
            Err(error) => Err(CreateDelegationTokenHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), CreateDelegationTokenHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(CreateDelegationTokenHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(CreateDelegationTokenHostError::ByteAccounting)?;
        Ok(())
    }
}
