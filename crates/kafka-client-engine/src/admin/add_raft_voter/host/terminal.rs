//! Call polling, route release, publication, reclamation, and recovery.

use kafka_client_core::{
    AddRaftVoterEffect, AddRaftVoterInput, AddRaftVoterState, DeliveryStatus, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    AddRaftVoterHandoff, AddRaftVoterHost, AddRaftVoterHostError, response::terminal_input,
};

impl AddRaftVoterHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, AddRaftVoterHostError> {
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
                .ok_or(AddRaftVoterHostError::InvalidHandoff)?;
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
            Err(_error) => Err(AddRaftVoterHostError::CallCompletion),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), AddRaftVoterHostError> {
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
                (AddRaftVoterState::Ready, _) => self.apply(
                    operation_id,
                    AddRaftVoterInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (AddRaftVoterState::AwaitingDriver, AddRaftVoterHandoff::Untouched) => {
                    self.apply(operation_id, AddRaftVoterInput::DriverRejected)?;
                }
                (AddRaftVoterState::AwaitingDriver, AddRaftVoterHandoff::HandedOff) => {
                    seal_call(self.operations[0].call.take());
                    self.apply(operation_id, AddRaftVoterInput::DriverAccepted)?;
                    self.apply(
                        operation_id,
                        AddRaftVoterInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (AddRaftVoterState::Submitted, AddRaftVoterHandoff::Submitted) => {
                    seal_call(self.operations[0].call.take());
                    self.apply(
                        operation_id,
                        AddRaftVoterInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (AddRaftVoterState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(AddRaftVoterHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), AddRaftVoterHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(AddRaftVoterHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(AddRaftVoterHostError::MissingTerminal)?;
            terminal_input(raw, operation.remaining_result_bytes)
        };
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(AddRaftVoterHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(AddRaftVoterHostError::MissingTerminal)?;
        raw.discard();
        match transition.into_effect() {
            Some(AddRaftVoterEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(AddRaftVoterHostError::MissingTerminal),
        }
    }

    pub(super) fn publish_terminal(&mut self, index: usize) -> Result<(), AddRaftVoterHostError> {
        if self.operations[index].call.is_some() || self.operations[index].raw_terminal.is_some() {
            return Err(AddRaftVoterHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(AddRaftVoterHostError::MissingTerminal)?;
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
                Err(AddRaftVoterHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, AddRaftVoterHostError> {
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
            Err(error) => Err(AddRaftVoterHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), AddRaftVoterHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(AddRaftVoterHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(AddRaftVoterHostError::ByteAccounting)?;
        Ok(())
    }
}

fn seal_call(call: Option<crate::driver::AddRaftVoterCall>) {
    if let Some(call) = call
        && let Some(recovered) = call.recover_after_driver_shutdown()
    {
        seal_recovered_call(recovered);
    }
}

fn seal_recovered_call(recovered: crate::driver::RecoveredAddRaftVoterCall) {
    recovered.seal();
}
