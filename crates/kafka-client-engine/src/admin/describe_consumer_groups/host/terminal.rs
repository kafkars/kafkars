//! Call polling, route release, publication, reclamation, and recovery.

#[cfg(test)]
mod test_support;

use kafka_client_core::AdminDescribeConsumerGroupsEffect;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    DescribeConsumerGroupsHost, DescribeConsumerGroupsHostError, response::terminal_input,
};

impl DescribeConsumerGroupsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, DescribeConsumerGroupsHostError> {
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
                .ok_or(DescribeConsumerGroupsHostError::InvalidHandoff)?;
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
            Err(_) => Err(DescribeConsumerGroupsHostError::CallCompletion),
        }
    }

    pub(super) fn settle_raw(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(DescribeConsumerGroupsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(DescribeConsumerGroupsHostError::MissingTerminal)?;
            if !operation.matches_raw(raw) {
                return Err(DescribeConsumerGroupsHostError::SubmissionMismatch);
            }
            terminal_input(raw)
        };
        let remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(DescribeConsumerGroupsHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let effect = transition
            .into_effect()
            .ok_or(DescribeConsumerGroupsHostError::MissingTerminal)?;
        if !effect_matches_operation(&self.operations[index], &effect) {
            return Err(DescribeConsumerGroupsHostError::SubmissionMismatch);
        }
        self.operations[index].remaining_result_bytes = remaining_result_bytes;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(DescribeConsumerGroupsHostError::MissingTerminal)?;
        raw.discard();
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(DescribeConsumerGroupsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DescribeConsumerGroupsHostError::MissingTerminal)?;
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
                Err(DescribeConsumerGroupsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DescribeConsumerGroupsHostError> {
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
            Err(error) => Err(DescribeConsumerGroupsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(DescribeConsumerGroupsHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DescribeConsumerGroupsHostError::ByteAccounting)?;
        Ok(())
    }
}

fn effect_matches_operation(
    operation: &super::DescribeConsumerGroupsOperation,
    effect: &AdminDescribeConsumerGroupsEffect,
) -> bool {
    match effect {
        AdminDescribeConsumerGroupsEffect::Submit {
            operation_id,
            deadline,
            group_id,
            include_authorized_operations,
            call_kind,
        } => {
            let route_index = operation
                .attempt
                .as_ref()
                .map_or(operation.route_index, |attempt| {
                    if attempt.group_id == *group_id {
                        operation.route_index
                    } else {
                        operation.route_index.saturating_add(1)
                    }
                });
            *operation_id == operation.operation_id
                && *deadline == operation.deadline.core()
                && operation.route_plan.group(route_index) == Some(group_id.as_str())
                && operation.route_plan.include_authorized_operations()
                    == *include_authorized_operations
                && operation.machine.current_group() == Some(group_id.as_str())
                && operation.machine.call_kind() == *call_kind
        }
        AdminDescribeConsumerGroupsEffect::Complete { operation_id, .. } => {
            *operation_id == operation.operation_id
        }
    }
}
