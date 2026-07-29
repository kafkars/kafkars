//! Call polling, publication, reclamation, and shutdown recovery.

mod recovery;

#[cfg(test)]
mod recovery_test;
#[cfg(test)]
mod test_support;

#[cfg(test)]
use kafka_client_core::LegacyAlterConfigsInput;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{LegacyAlterConfigsHost, LegacyAlterConfigsHostError, response::terminal_input};

impl LegacyAlterConfigsHost {
    #[cfg(test)]
    pub(in crate::admin::legacy_alter_configs) fn apply_response_for_test(
        &mut self,
        operation_id: kafka_client_core::OperationId,
        batch: kafka_client_core::LegacyAlterConfigsBatch,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(LegacyAlterConfigsHostError::UnknownOperation)?;
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(self.operations[index].active_result_contribution)
            .ok_or(LegacyAlterConfigsHostError::ByteAccounting)?;
        self.apply(
            operation_id,
            LegacyAlterConfigsInput::BrokerResponded { batch },
        )
    }

    pub(super) fn poll_one_call(&mut self) -> Result<bool, LegacyAlterConfigsHostError> {
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
                .ok_or(LegacyAlterConfigsHostError::InvalidHandoff)?;
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
            Err(_error) => Err(LegacyAlterConfigsHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), LegacyAlterConfigsHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(LegacyAlterConfigsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(LegacyAlterConfigsHostError::MissingTerminal)?;
            if !operation.matches_correlation(raw.route(), raw.plan()) {
                return Err(LegacyAlterConfigsHostError::SubmissionMismatch);
            }
            terminal_input(
                raw,
                operation.active_result_limit,
                operation.active_result_contribution,
            )
        };
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(LegacyAlterConfigsHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(LegacyAlterConfigsHostError::MissingTerminal)?;
        raw.discard();
        let effect = transition
            .into_effect()
            .ok_or(LegacyAlterConfigsHostError::MissingTerminal)?;
        self.apply_effect(index, Some(effect))
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(LegacyAlterConfigsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(LegacyAlterConfigsHostError::MissingTerminal)?;
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
                Err(LegacyAlterConfigsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, LegacyAlterConfigsHostError> {
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
            Err(error) => Err(LegacyAlterConfigsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(LegacyAlterConfigsHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(LegacyAlterConfigsHostError::ByteAccounting)?;
        Ok(())
    }
}
