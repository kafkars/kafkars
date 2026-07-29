//! Call polling, publication, reclamation, and recovery routing.

mod recovery;

#[cfg(test)]
mod test_support;

use kafka_client_core::UnregisterBrokerEffect;

use crate::{
    completion::{CompletionRegistryError, ReclaimStatus},
    driver::DriverOwner,
};

use super::{
    UnregisterBrokerHost, UnregisterBrokerHostError, UnregisterBrokerOperation,
    response::terminal_input,
};

impl UnregisterBrokerHost {
    pub(super) fn poll_one_call(
        &mut self,
        driver: Option<&DriverOwner>,
    ) -> Result<bool, UnregisterBrokerHostError> {
        if let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.raw_terminal.is_some())
        {
            let ready = self.operations[index]
                .raw_terminal
                .as_mut()
                .ok_or(UnregisterBrokerHostError::MissingTerminal)?
                .poll_controller_refresh(driver)
                .ok_or(UnregisterBrokerHostError::DriverMissing)?;
            if ready {
                self.settle_raw(index)?;
            }
            return Ok(true);
        }
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
            .ok_or(UnregisterBrokerHostError::InvalidHandoff)?
            .try_terminal();
        let Some(terminal) = terminal else {
            return Ok(false);
        };
        match terminal {
            Ok(terminal) => {
                drop(self.operations[index].call.take());
                self.operations[index].raw_terminal = Some(terminal);
                Ok(true)
            }
            Err(_error) => Err(UnregisterBrokerHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), UnregisterBrokerHostError> {
        let operation = self
            .operations
            .get_mut(index)
            .ok_or(UnregisterBrokerHostError::UnknownOperation)?;
        settle_operation(operation)?;
        self.publish_terminal(index)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), UnregisterBrokerHostError> {
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
                Err(UnregisterBrokerHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, UnregisterBrokerHostError> {
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
            Err(error) => Err(UnregisterBrokerHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), UnregisterBrokerHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(UnregisterBrokerHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(UnregisterBrokerHostError::ByteAccounting)?;
        Ok(())
    }
}

fn settle_operation(
    operation: &mut UnregisterBrokerOperation,
) -> Result<(), UnregisterBrokerHostError> {
    let raw = operation
        .raw_terminal
        .as_ref()
        .ok_or(UnregisterBrokerHostError::MissingTerminal)?;
    let (input, retained_bytes) = terminal_input(raw, operation.remaining_result_bytes);
    operation.remaining_result_bytes = operation
        .remaining_result_bytes
        .checked_sub(retained_bytes)
        .ok_or(UnregisterBrokerHostError::ByteAccounting)?;
    let transition = operation.machine.apply(input)?;
    let raw = operation
        .raw_terminal
        .take()
        .ok_or(UnregisterBrokerHostError::MissingTerminal)?;
    raw.discard();
    match transition.into_effect() {
        Some(UnregisterBrokerEffect::Complete {
            operation_id,
            terminal,
        }) if operation_id == operation.operation_id => {
            operation.terminal = Some(terminal);
            Ok(())
        }
        _ => Err(UnregisterBrokerHostError::MissingTerminal),
    }
}

fn take_publishable_terminal(
    operation: &mut UnregisterBrokerOperation,
) -> Result<
    (
        crate::completion::CompletionId,
        kafka_client_core::UnregisterBrokerTerminal,
    ),
    UnregisterBrokerHostError,
> {
    if operation.call.is_some()
        || operation.recovered_call.is_some()
        || operation.raw_terminal.is_some()
    {
        return Err(UnregisterBrokerHostError::InvalidHandoff);
    }
    let terminal = operation
        .terminal
        .take()
        .ok_or(UnregisterBrokerHostError::MissingTerminal)?;
    Ok((operation.completion_id, terminal))
}

fn restore_terminal(
    operation: &mut UnregisterBrokerOperation,
    terminal: kafka_client_core::UnregisterBrokerTerminal,
) {
    operation.terminal = Some(terminal);
}
