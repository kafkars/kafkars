//! Call polling, route release, publication, reclamation, and recovery.

mod recovery;

use kafka_client_core::DescribeAclsEffect;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{DescribeAclsHost, DescribeAclsHostError, response::terminal_input};

impl DescribeAclsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, DescribeAclsHostError> {
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
                .ok_or(DescribeAclsHostError::InvalidHandoff)?;
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
            Err(_error) => Err(DescribeAclsHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), DescribeAclsHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(DescribeAclsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(DescribeAclsHostError::MissingTerminal)?;
            if !operation.matches_raw(raw) {
                return Err(DescribeAclsHostError::SubmissionMismatch);
            }
            terminal_input(raw)
        };
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(DescribeAclsHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(DescribeAclsHostError::MissingTerminal)?;
        raw.discard();
        match transition.into_effect() {
            Some(DescribeAclsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(DescribeAclsHostError::MissingTerminal),
        }
    }

    pub(super) fn publish_terminal(&mut self, index: usize) -> Result<(), DescribeAclsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(DescribeAclsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DescribeAclsHostError::MissingTerminal)?;
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
                Err(DescribeAclsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DescribeAclsHostError> {
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
            Err(error) => Err(DescribeAclsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DescribeAclsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(DescribeAclsHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DescribeAclsHostError::ByteAccounting)?;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::admin::describe_acls) fn retain_recovered_call_for_test(&mut self) {
        let operation = &mut self.operations[0];
        operation.recovered_call = Some(crate::driver::RecoveredDescribeAclsCall::for_test(
            operation.plan.clone(),
            operation.result_limit,
        ));
    }

    #[cfg(test)]
    pub(in crate::admin::describe_acls) fn recovered_call_is_retained_for_test(&self) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    #[cfg(test)]
    pub(in crate::admin::describe_acls) fn retain_raw_terminal_for_test(
        &mut self,
        plan: kafka_client_core::DescribeAclsPlan,
        result_limit: usize,
    ) {
        self.operations[0].raw_terminal = Some(crate::driver::DescribeAclsRawTerminal::for_test(
            plan,
            result_limit,
        ));
    }

    #[cfg(test)]
    pub(in crate::admin::describe_acls) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DescribeAclsHostError> {
        self.settle_recovered_transport(0)
    }

    #[cfg(test)]
    pub(in crate::admin::describe_acls) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), DescribeAclsHostError> {
        self.publish_terminal(0)
    }
}
