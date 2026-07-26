//! Call polling, deliberate route release, publication, reclamation, and recovery.

use kafka_client_core::{
    DeliveryStatus, ListConsumerGroupOffsetsEffect, ListConsumerGroupOffsetsInput,
    ListConsumerGroupOffsetsState, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    ListConsumerGroupOffsetsHandoff, ListConsumerGroupOffsetsHost,
    ListConsumerGroupOffsetsHostError, response::terminal_input,
};

impl ListConsumerGroupOffsetsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, ListConsumerGroupOffsetsHostError> {
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
                .ok_or(ListConsumerGroupOffsetsHostError::InvalidHandoff)?;
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
            Err(_error) => Err(ListConsumerGroupOffsetsHostError::CallCompletion),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
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
                (ListConsumerGroupOffsetsState::Ready, _) => self.apply(
                    operation_id,
                    ListConsumerGroupOffsetsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    ListConsumerGroupOffsetsState::AwaitingDriver,
                    ListConsumerGroupOffsetsHandoff::Untouched,
                ) => self.apply(operation_id, ListConsumerGroupOffsetsInput::DriverRejected)?,
                (
                    ListConsumerGroupOffsetsState::AwaitingDriver,
                    ListConsumerGroupOffsetsHandoff::HandedOff,
                ) => {
                    if let Some(call) = self.operations[0].call.take() {
                        if let Some(recovered) = call.recover_after_driver_shutdown() {
                            recovered.seal();
                        }
                    }
                    self.apply(operation_id, ListConsumerGroupOffsetsInput::DriverAccepted)?;
                    self.apply(
                        operation_id,
                        ListConsumerGroupOffsetsInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (
                    ListConsumerGroupOffsetsState::Submitted,
                    ListConsumerGroupOffsetsHandoff::Submitted,
                ) => {
                    if let Some(call) = self.operations[0].call.take() {
                        if let Some(recovered) = call.recover_after_driver_shutdown() {
                            recovered.seal();
                        }
                    }
                    self.apply(
                        operation_id,
                        ListConsumerGroupOffsetsInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (ListConsumerGroupOffsetsState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(ListConsumerGroupOffsetsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), ListConsumerGroupOffsetsHostError> {
        let input = {
            let operation = self
                .operations
                .get(index)
                .ok_or(ListConsumerGroupOffsetsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(ListConsumerGroupOffsetsHostError::MissingTerminal)?;
            terminal_input(raw, &operation.group_id, operation.result_limit)
        };
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(ListConsumerGroupOffsetsHostError::MissingTerminal)?;
        raw.discard();
        match transition.into_effect() {
            Some(ListConsumerGroupOffsetsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(ListConsumerGroupOffsetsHostError::MissingTerminal),
        }
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        if self.operations[index].call.is_some() || self.operations[index].raw_terminal.is_some() {
            return Err(ListConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(ListConsumerGroupOffsetsHostError::MissingTerminal)?;
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
                Err(ListConsumerGroupOffsetsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, ListConsumerGroupOffsetsHostError> {
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
            Err(error) => Err(ListConsumerGroupOffsetsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(ListConsumerGroupOffsetsHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(ListConsumerGroupOffsetsHostError::ByteAccounting)?;
        Ok(())
    }
}
