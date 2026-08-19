//! Call polling, route release, publication, reclamation, and recovery.

mod recovery;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{DescribeLogDirsHost, DescribeLogDirsHostError, response::terminal_input};

impl DescribeLogDirsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, DescribeLogDirsHostError> {
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
                .ok_or(DescribeLogDirsHostError::InvalidHandoff)?;
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
            Err(_) => Err(DescribeLogDirsHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), DescribeLogDirsHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(DescribeLogDirsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(DescribeLogDirsHostError::MissingTerminal)?;
            let current = operation
                .machine
                .current_broker()
                .ok_or(DescribeLogDirsHostError::SubmissionMismatch)?;
            if !raw.matches(
                current,
                operation.plan.selection(),
                operation.request_scratch_limit,
                operation.result_limit,
            ) {
                return Err(DescribeLogDirsHostError::SubmissionMismatch);
            }
            terminal_input(raw, operation.plan.selection())
        };
        let remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(DescribeLogDirsHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let effect = transition
            .into_effect()
            .ok_or(DescribeLogDirsHostError::MissingTerminal)?;
        self.validate_effect(index, &effect)?;
        self.operations[index].remaining_result_bytes = remaining_result_bytes;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(DescribeLogDirsHostError::MissingTerminal)?;
        raw.discard();
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeLogDirsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(DescribeLogDirsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DescribeLogDirsHostError::MissingTerminal)?;
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
                Err(DescribeLogDirsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DescribeLogDirsHostError> {
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
            Err(error) => Err(DescribeLogDirsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DescribeLogDirsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(DescribeLogDirsHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DescribeLogDirsHostError::ByteAccounting)?;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::admin::describe_log_dirs) fn retain_recovered_call_for_test(&mut self) {
        let (broker_id, selection, request_scratch_limit, result_limit) = {
            let operation = &self.operations[0];
            (
                operation
                    .machine
                    .current_broker()
                    .unwrap_or_else(|| panic!("current broker")),
                operation.plan.selection().clone(),
                operation.request_scratch_limit,
                operation.result_limit,
            )
        };
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredDescribeLogDirsCall::for_test(
                broker_id,
                selection,
                request_scratch_limit,
                result_limit,
            ));
    }

    #[cfg(test)]
    pub(in crate::admin::describe_log_dirs) fn recovered_call_is_retained_for_test(&self) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    #[cfg(test)]
    pub(in crate::admin::describe_log_dirs) fn settle_matching_raw_for_test(
        &mut self,
    ) -> Result<(), DescribeLogDirsHostError> {
        let operation_id = self.operations[0].operation_id;
        self.apply(
            operation_id,
            kafka_client_core::AdminDescribeLogDirsInput::DriverAccepted,
        )?;
        let (broker_id, selection, request_scratch_limit, result_limit) = {
            let operation = &self.operations[0];
            (
                operation
                    .machine
                    .current_broker()
                    .ok_or(DescribeLogDirsHostError::SubmissionMismatch)?,
                operation.plan.selection().clone(),
                operation.request_scratch_limit,
                operation.result_limit,
            )
        };
        self.operations[0].raw_terminal =
            Some(crate::driver::DescribeLogDirsRawTerminal::for_test(
                broker_id,
                selection,
                request_scratch_limit,
                result_limit,
            ));
        self.settle_raw(0)
    }

    #[cfg(test)]
    pub(in crate::admin::describe_log_dirs) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DescribeLogDirsHostError> {
        self.settle_recovered_transport(0)
    }
}
