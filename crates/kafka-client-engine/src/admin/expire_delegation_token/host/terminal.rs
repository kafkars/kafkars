//! Call polling, route release, publication, reclamation, and recovery.

mod recovery;

#[cfg(test)]
mod test_support;

use kafka_client_core::ExpireDelegationTokenEffect;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    ExpireDelegationTokenHost, ExpireDelegationTokenHostError, ExpireDelegationTokenOperation,
    response::terminal_input,
};

impl ExpireDelegationTokenHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, ExpireDelegationTokenHostError> {
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
            .ok_or(ExpireDelegationTokenHostError::InvalidHandoff)?
            .try_terminal();
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
            Err(_error) => Err(ExpireDelegationTokenHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), ExpireDelegationTokenHostError> {
        let operation = self
            .operations
            .get_mut(index)
            .ok_or(ExpireDelegationTokenHostError::UnknownOperation)?;
        settle_operation(operation)?;
        self.publish_terminal(index)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), ExpireDelegationTokenHostError> {
        let (completion_id, terminal) = take_publishable_terminal(&mut self.operations[index])?;
        match self.completions.publish(completion_id, terminal) {
            Ok(()) => {
                let operation = self.operations.remove(index);
                self.published_bytes
                    .push((operation.completion_id, operation.retained_bytes));
                Ok(())
            }
            Err((error, terminal)) => {
                restore_terminal(&mut self.operations[index], terminal);
                Err(ExpireDelegationTokenHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, ExpireDelegationTokenHostError> {
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
            Err(error) => Err(ExpireDelegationTokenHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), ExpireDelegationTokenHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(ExpireDelegationTokenHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(ExpireDelegationTokenHostError::ByteAccounting)?;
        Ok(())
    }
}

fn settle_operation(
    operation: &mut ExpireDelegationTokenOperation,
) -> Result<(), ExpireDelegationTokenHostError> {
    let raw = operation
        .raw_terminal
        .as_ref()
        .ok_or(ExpireDelegationTokenHostError::MissingTerminal)?;
    let (input, retained_bytes) = terminal_input(raw, operation.remaining_result_bytes);
    operation.remaining_result_bytes = operation
        .remaining_result_bytes
        .checked_sub(retained_bytes)
        .ok_or(ExpireDelegationTokenHostError::ByteAccounting)?;
    let machine = &mut operation.machine;
    let transition = machine.apply(input)?;
    let raw = operation
        .raw_terminal
        .take()
        .ok_or(ExpireDelegationTokenHostError::MissingTerminal)?;
    raw.discard();
    match transition.into_effect() {
        Some(ExpireDelegationTokenEffect::Complete {
            operation_id,
            terminal,
        }) if operation_id == operation.operation_id => {
            operation.terminal = Some(terminal);
            Ok(())
        }
        _ => Err(ExpireDelegationTokenHostError::MissingTerminal),
    }
}

fn take_publishable_terminal(
    operation: &mut ExpireDelegationTokenOperation,
) -> Result<
    (
        crate::completion::CompletionId,
        kafka_client_core::ExpireDelegationTokenTerminal,
    ),
    ExpireDelegationTokenHostError,
> {
    if operation.call.is_some()
        || operation.recovered_call.is_some()
        || operation.raw_terminal.is_some()
    {
        return Err(ExpireDelegationTokenHostError::InvalidHandoff);
    }
    let terminal = operation
        .terminal
        .take()
        .ok_or(ExpireDelegationTokenHostError::MissingTerminal)?;
    Ok((operation.completion_id, terminal))
}

fn restore_terminal(
    operation: &mut ExpireDelegationTokenOperation,
    terminal: kafka_client_core::ExpireDelegationTokenTerminal,
) {
    operation.terminal = Some(terminal);
}
