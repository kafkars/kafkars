//! Call polling, route release, publication, reclamation, and recovery.

#[cfg(test)]
mod test_support;

use kafka_client_core::AlterReplicaLogDirsEffect;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{AlterReplicaLogDirsHost, AlterReplicaLogDirsHostError, response::terminal_input};

impl AlterReplicaLogDirsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, AlterReplicaLogDirsHostError> {
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
                .ok_or(AlterReplicaLogDirsHostError::InvalidHandoff)?;
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
            Err(_) => Err(AlterReplicaLogDirsHostError::CallCompletion),
        }
    }

    pub(super) fn settle_raw(&mut self, index: usize) -> Result<(), AlterReplicaLogDirsHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(AlterReplicaLogDirsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(AlterReplicaLogDirsHostError::MissingTerminal)?;
            if !operation.matches_raw(raw) {
                return Err(AlterReplicaLogDirsHostError::SubmissionMismatch);
            }
            terminal_input(raw)
        };
        let remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(AlterReplicaLogDirsHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let effect = transition
            .into_effect()
            .ok_or(AlterReplicaLogDirsHostError::MissingTerminal)?;
        if !effect_matches_operation(&self.operations[index], &effect) {
            return Err(AlterReplicaLogDirsHostError::SubmissionMismatch);
        }
        self.operations[index].remaining_result_bytes = remaining_result_bytes;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(AlterReplicaLogDirsHostError::MissingTerminal)?;
        raw.discard();
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), AlterReplicaLogDirsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(AlterReplicaLogDirsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(AlterReplicaLogDirsHostError::MissingTerminal)?;
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
                Err(AlterReplicaLogDirsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, AlterReplicaLogDirsHostError> {
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
            Err(error) => Err(AlterReplicaLogDirsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), AlterReplicaLogDirsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(AlterReplicaLogDirsHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(AlterReplicaLogDirsHostError::ByteAccounting)?;
        Ok(())
    }
}

fn effect_matches_operation(
    operation: &super::AlterReplicaLogDirsOperation,
    effect: &AlterReplicaLogDirsEffect,
) -> bool {
    match effect {
        AlterReplicaLogDirsEffect::Submit {
            operation_id,
            deadline,
            broker_id,
            assignments,
        } => {
            *operation_id == operation.operation_id
                && *deadline == operation.deadline.core()
                && operation.machine.current_broker() == Some(*broker_id)
                && !assignments.is_empty()
                && assignments
                    .iter()
                    .all(|assignment| assignment.broker_id() == *broker_id)
        }
        AlterReplicaLogDirsEffect::Complete { operation_id, .. } => {
            *operation_id == operation.operation_id
        }
    }
}
