//! Call polling, publication, reclamation, and shutdown recovery.

mod recovery;

#[cfg(test)]
mod recovery_test;
#[cfg(test)]
mod test_support;

use kafka_client_core::AbortPartitionTransactionEffect;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    AbortPartitionTransactionHost, AbortPartitionTransactionHostError, response::terminal_input,
};

impl AbortPartitionTransactionHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, AbortPartitionTransactionHostError> {
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
                .ok_or(AbortPartitionTransactionHostError::InvalidHandoff)?;
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
            Err(_error) => Err(AbortPartitionTransactionHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), AbortPartitionTransactionHostError> {
        let input = {
            let operation = self
                .operations
                .get(index)
                .ok_or(AbortPartitionTransactionHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(AbortPartitionTransactionHostError::MissingTerminal)?;
            terminal_input(raw, &operation.plan)
        };
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(AbortPartitionTransactionHostError::MissingTerminal)?;
        raw.discard();
        match transition.into_effect() {
            Some(AbortPartitionTransactionEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(AbortPartitionTransactionHostError::MissingTerminal),
        }
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), AbortPartitionTransactionHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(AbortPartitionTransactionHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(AbortPartitionTransactionHostError::MissingTerminal)?;
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
                Err(AbortPartitionTransactionHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, AbortPartitionTransactionHostError> {
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
            Err(error) => Err(AbortPartitionTransactionHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), AbortPartitionTransactionHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(AbortPartitionTransactionHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(AbortPartitionTransactionHostError::ByteAccounting)?;
        Ok(())
    }
}
