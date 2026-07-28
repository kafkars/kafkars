//! Call polling, route release, publication, reclamation, and recovery.

use kafka_client_core::{
    AdminDescribeLogDirsInput, AdminDescribeLogDirsState, DeliveryStatus, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    DescribeLogDirsHandoff, DescribeLogDirsHost, DescribeLogDirsHostError, response::terminal_input,
};

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

    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), DescribeLogDirsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            let state = operation.machine.state();
            let handoff = operation.handoff;
            match (state, handoff) {
                (AdminDescribeLogDirsState::Ready, _) => self.apply(
                    operation_id,
                    AdminDescribeLogDirsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (AdminDescribeLogDirsState::AwaitingDriver, DescribeLogDirsHandoff::Untouched) => {
                    self.apply(operation_id, AdminDescribeLogDirsInput::DriverRejected)?;
                }
                (AdminDescribeLogDirsState::AwaitingDriver, DescribeLogDirsHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, AdminDescribeLogDirsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (AdminDescribeLogDirsState::Submitted, DescribeLogDirsHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AdminDescribeLogDirsState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(DescribeLogDirsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), DescribeLogDirsHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(DescribeLogDirsHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    fn settle_recovered_transport(&mut self, index: usize) -> Result<(), DescribeLogDirsHostError> {
        let transition =
            self.operations[index]
                .machine
                .apply(AdminDescribeLogDirsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let effect = transition
            .into_effect()
            .ok_or(DescribeLogDirsHostError::MissingTerminal)?;
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DescribeLogDirsHostError::InvalidHandoff)?;
        recovered.seal();
        self.install_effect(index, effect)
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
            terminal_input(raw, current, operation.remaining_result_bytes)
        };
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(DescribeLogDirsHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(DescribeLogDirsHostError::MissingTerminal)?;
        raw.discard();
        let effect = transition
            .into_effect()
            .ok_or(DescribeLogDirsHostError::MissingTerminal)?;
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeLogDirsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
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
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredDescribeLogDirsCall::for_test());
    }

    #[cfg(test)]
    pub(in crate::admin::describe_log_dirs) fn recovered_call_is_retained_for_test(&self) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    #[cfg(test)]
    pub(in crate::admin::describe_log_dirs) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DescribeLogDirsHostError> {
        self.settle_recovered_transport(0)
    }
}
