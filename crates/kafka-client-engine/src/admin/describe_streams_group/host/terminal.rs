//! Call polling, publication, reclamation, and shutdown recovery.

use kafka_client_core::{
    DeliveryStatus, DescribeStreamsGroupEffect, DescribeStreamsGroupInput,
    DescribeStreamsGroupState, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    DescribeStreamsGroupHandoff, DescribeStreamsGroupHost, DescribeStreamsGroupHostError,
    response::terminal_input,
};

impl DescribeStreamsGroupHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, DescribeStreamsGroupHostError> {
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
                .ok_or(DescribeStreamsGroupHostError::InvalidHandoff)?;
            call.try_terminal()
        };
        let Some(terminal) = terminal else {
            return Ok(false);
        };
        drop(self.operations[index].call.take());
        match terminal {
            Ok(terminal) => {
                self.operations[index].raw_terminal = Some(terminal);
                self.settle_raw(index)?;
                Ok(true)
            }
            Err(_error) => Err(DescribeStreamsGroupHostError::CallCompletion),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DescribeStreamsGroupHostError> {
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
                (DescribeStreamsGroupState::Ready, _) => self.apply(
                    operation_id,
                    DescribeStreamsGroupInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    DescribeStreamsGroupState::AwaitingDriver,
                    DescribeStreamsGroupHandoff::Untouched,
                ) => self.apply(operation_id, DescribeStreamsGroupInput::DriverRejected)?,
                (
                    DescribeStreamsGroupState::AwaitingDriver,
                    DescribeStreamsGroupHandoff::HandedOff,
                ) => {
                    seal_call(self.operations[0].call.take());
                    self.apply(operation_id, DescribeStreamsGroupInput::DriverAccepted)?;
                    self.apply(
                        operation_id,
                        DescribeStreamsGroupInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (DescribeStreamsGroupState::Submitted, DescribeStreamsGroupHandoff::Submitted) => {
                    seal_call(self.operations[0].call.take());
                    self.apply(
                        operation_id,
                        DescribeStreamsGroupInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (DescribeStreamsGroupState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(DescribeStreamsGroupHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), DescribeStreamsGroupHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(DescribeStreamsGroupHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(DescribeStreamsGroupHostError::MissingTerminal)?;
            let plan = operation
                .active_plan
                .as_ref()
                .ok_or(DescribeStreamsGroupHostError::SubmissionMismatch)?;
            terminal_input(raw, plan, operation.remaining_result_bytes)
        };
        let effect = self.apply_settled_input(index, input, retained_bytes)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(DescribeStreamsGroupHostError::MissingTerminal)?;
        raw.discard();
        self.install_effect(index, effect)
    }

    fn apply_settled_input(
        &mut self,
        index: usize,
        input: DescribeStreamsGroupInput,
        retained_bytes: usize,
    ) -> Result<DescribeStreamsGroupEffect, DescribeStreamsGroupHostError> {
        if self.operations[index].handoff != DescribeStreamsGroupHandoff::Submitted {
            return Err(DescribeStreamsGroupHostError::InvalidHandoff);
        }
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(DescribeStreamsGroupHostError::ByteAccounting)?;
        self.operations[index]
            .active_plan
            .take()
            .ok_or(DescribeStreamsGroupHostError::SubmissionMismatch)?;
        self.operations[index]
            .machine
            .apply(input)?
            .into_effect()
            .ok_or(DescribeStreamsGroupHostError::MissingTerminal)
    }

    #[cfg(test)]
    pub(crate) fn settle_current_for_test(
        &mut self,
        operation_id: kafka_client_core::OperationId,
        input: DescribeStreamsGroupInput,
        retained_bytes: usize,
    ) -> Result<(), DescribeStreamsGroupHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeStreamsGroupHostError::UnknownOperation)?;
        self.apply(operation_id, DescribeStreamsGroupInput::DriverAccepted)?;
        let effect = self.apply_settled_input(index, input, retained_bytes)?;
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeStreamsGroupHostError> {
        if self.operations[index].call.is_some() || self.operations[index].raw_terminal.is_some() {
            return Err(DescribeStreamsGroupHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DescribeStreamsGroupHostError::MissingTerminal)?;
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
                Err(DescribeStreamsGroupHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DescribeStreamsGroupHostError> {
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
            Err(error) => Err(DescribeStreamsGroupHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DescribeStreamsGroupHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(DescribeStreamsGroupHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DescribeStreamsGroupHostError::ByteAccounting)?;
        Ok(())
    }
}

fn seal_call(call: Option<crate::driver::DescribeStreamsGroupCall>) {
    if let Some(call) = call {
        if let Some(recovered) = call.recover_after_driver_shutdown() {
            recovered.seal();
        }
    }
}
