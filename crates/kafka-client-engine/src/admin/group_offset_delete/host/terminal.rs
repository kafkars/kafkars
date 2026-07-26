//! Call polling, receipt release, publication, reclamation, and recovery.

use kafka_client_core::{
    DeleteConsumerGroupOffsetsEffect, DeleteConsumerGroupOffsetsInput,
    DeleteConsumerGroupOffsetsState, DeliveryStatus, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    DeleteConsumerGroupOffsetsHandoff, DeleteConsumerGroupOffsetsHost,
    DeleteConsumerGroupOffsetsHostError, response::terminal_input,
};

impl DeleteConsumerGroupOffsetsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, DeleteConsumerGroupOffsetsHostError> {
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
                .ok_or(DeleteConsumerGroupOffsetsHostError::InvalidHandoff)?;
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
            Err(_error) => Err(DeleteConsumerGroupOffsetsHostError::CallCompletion),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
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
                (DeleteConsumerGroupOffsetsState::Ready, _) => self.apply(
                    operation_id,
                    DeleteConsumerGroupOffsetsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    DeleteConsumerGroupOffsetsState::AwaitingDriver,
                    DeleteConsumerGroupOffsetsHandoff::Untouched,
                ) => self.apply(
                    operation_id,
                    DeleteConsumerGroupOffsetsInput::DriverRejected,
                )?,
                (
                    DeleteConsumerGroupOffsetsState::AwaitingDriver,
                    DeleteConsumerGroupOffsetsHandoff::HandedOff,
                ) => {
                    self.recover_call(0);
                    self.apply(
                        operation_id,
                        DeleteConsumerGroupOffsetsInput::DriverAccepted,
                    )?;
                    self.apply(
                        operation_id,
                        DeleteConsumerGroupOffsetsInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (
                    DeleteConsumerGroupOffsetsState::Submitted,
                    DeleteConsumerGroupOffsetsHandoff::Submitted,
                ) => {
                    self.recover_call(0);
                    self.apply(
                        operation_id,
                        DeleteConsumerGroupOffsetsInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (DeleteConsumerGroupOffsetsState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(DeleteConsumerGroupOffsetsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn recover_call(&mut self, index: usize) {
        if let Some(call) = self.operations[index].call.take() {
            if let Some(recovered) = call.recover_after_driver_shutdown() {
                recovered.seal();
            }
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
        let input = {
            let operation = self
                .operations
                .get(index)
                .ok_or(DeleteConsumerGroupOffsetsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(DeleteConsumerGroupOffsetsHostError::MissingTerminal)?;
            terminal_input(raw, &operation.response_plan, operation.result_limit)
        };
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(DeleteConsumerGroupOffsetsHostError::MissingTerminal)?;
        raw.discard();
        match transition.into_effect() {
            Some(DeleteConsumerGroupOffsetsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(DeleteConsumerGroupOffsetsHostError::MissingTerminal),
        }
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
        if self.operations[index].call.is_some() || self.operations[index].raw_terminal.is_some() {
            return Err(DeleteConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DeleteConsumerGroupOffsetsHostError::MissingTerminal)?;
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
                Err(DeleteConsumerGroupOffsetsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DeleteConsumerGroupOffsetsHostError> {
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
            Err(error) => Err(DeleteConsumerGroupOffsetsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DeleteConsumerGroupOffsetsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(DeleteConsumerGroupOffsetsHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DeleteConsumerGroupOffsetsHostError::ByteAccounting)?;
        Ok(())
    }
}
