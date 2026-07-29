//! Call polling, route release, publication, reclamation, and recovery.

use kafka_client_core::{
    DeliveryStatus, DescribeMetadataQuorumEffect, DescribeMetadataQuorumInput,
    DescribeMetadataQuorumState, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    DescribeMetadataQuorumHandoff, DescribeMetadataQuorumHost, DescribeMetadataQuorumHostError,
    response::terminal_input,
};

impl DescribeMetadataQuorumHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, DescribeMetadataQuorumHostError> {
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
                .ok_or(DescribeMetadataQuorumHostError::InvalidHandoff)?;
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
            Err(_error) => Err(DescribeMetadataQuorumHostError::CallCompletion),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DescribeMetadataQuorumHostError> {
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
                (DescribeMetadataQuorumState::Ready, _) => self.apply(
                    operation_id,
                    DescribeMetadataQuorumInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    DescribeMetadataQuorumState::AwaitingDriver,
                    DescribeMetadataQuorumHandoff::Untouched,
                ) => {
                    self.apply(operation_id, DescribeMetadataQuorumInput::DriverRejected)?;
                }
                (
                    DescribeMetadataQuorumState::AwaitingDriver,
                    DescribeMetadataQuorumHandoff::HandedOff,
                ) => {
                    seal_call(self.operations[0].call.take());
                    self.apply(operation_id, DescribeMetadataQuorumInput::DriverAccepted)?;
                    self.apply(
                        operation_id,
                        DescribeMetadataQuorumInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (
                    DescribeMetadataQuorumState::Submitted,
                    DescribeMetadataQuorumHandoff::Submitted,
                ) => {
                    seal_call(self.operations[0].call.take());
                    self.apply(
                        operation_id,
                        DescribeMetadataQuorumInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (DescribeMetadataQuorumState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(DescribeMetadataQuorumHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), DescribeMetadataQuorumHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(DescribeMetadataQuorumHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(DescribeMetadataQuorumHostError::MissingTerminal)?;
            terminal_input(raw, operation.remaining_result_bytes)
        };
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(DescribeMetadataQuorumHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(DescribeMetadataQuorumHostError::MissingTerminal)?;
        raw.discard();
        match transition.into_effect() {
            Some(DescribeMetadataQuorumEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(DescribeMetadataQuorumHostError::MissingTerminal),
        }
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeMetadataQuorumHostError> {
        if self.operations[index].call.is_some() || self.operations[index].raw_terminal.is_some() {
            return Err(DescribeMetadataQuorumHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DescribeMetadataQuorumHostError::MissingTerminal)?;
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
                Err(DescribeMetadataQuorumHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DescribeMetadataQuorumHostError> {
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
            Err(error) => Err(DescribeMetadataQuorumHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DescribeMetadataQuorumHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(DescribeMetadataQuorumHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DescribeMetadataQuorumHostError::ByteAccounting)?;
        Ok(())
    }
}

fn seal_call(call: Option<crate::driver::DescribeMetadataQuorumCall>) {
    if let Some(call) = call {
        if let Some(recovered) = call.recover_after_driver_shutdown() {
            recovered.seal();
        }
    }
}
