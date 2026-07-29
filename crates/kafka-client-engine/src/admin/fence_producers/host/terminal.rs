//! Call polling, route release, publication, reclamation, and recovery.

use kafka_client_core::{
    AdminFenceProducersInput, AdminFenceProducersState, DeliveryStatus, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    AdminFenceProducersHandoff, AdminFenceProducersHost, AdminFenceProducersHostError,
    response::terminal_input,
};

impl AdminFenceProducersHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, AdminFenceProducersHostError> {
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
                .ok_or(AdminFenceProducersHostError::InvalidHandoff)?;
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
            Err(_error) => Err(AdminFenceProducersHostError::CallCompletion),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), AdminFenceProducersHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (AdminFenceProducersState::Ready, _) => self.apply(
                    operation_id,
                    AdminFenceProducersInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    AdminFenceProducersState::AwaitingDriver,
                    AdminFenceProducersHandoff::Untouched,
                ) => self.apply(operation_id, AdminFenceProducersInput::DriverRejected)?,
                (
                    AdminFenceProducersState::AwaitingDriver,
                    AdminFenceProducersHandoff::HandedOff,
                ) => {
                    seal_call(self.operations[0].call.take());
                    self.apply(operation_id, AdminFenceProducersInput::DriverAccepted)?;
                    self.apply(
                        operation_id,
                        AdminFenceProducersInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (AdminFenceProducersState::Submitted, AdminFenceProducersHandoff::Submitted) => {
                    seal_call(self.operations[0].call.take());
                    self.apply(
                        operation_id,
                        AdminFenceProducersInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (AdminFenceProducersState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(AdminFenceProducersHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), AdminFenceProducersHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(AdminFenceProducersHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(AdminFenceProducersHostError::MissingTerminal)?;
            let transactional_id = operation
                .machine
                .current_transactional_id()
                .ok_or(AdminFenceProducersHostError::SubmissionMismatch)?;
            terminal_input(raw, transactional_id, operation.remaining_result_bytes)
        };
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(AdminFenceProducersHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(AdminFenceProducersHostError::MissingTerminal)?;
        raw.discard();
        let effect = transition
            .into_effect()
            .ok_or(AdminFenceProducersHostError::MissingTerminal)?;
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), AdminFenceProducersHostError> {
        if self.operations[index].call.is_some() || self.operations[index].raw_terminal.is_some() {
            return Err(AdminFenceProducersHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(AdminFenceProducersHostError::MissingTerminal)?;
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
                Err(AdminFenceProducersHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, AdminFenceProducersHostError> {
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
            Err(error) => Err(AdminFenceProducersHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), AdminFenceProducersHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(AdminFenceProducersHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(AdminFenceProducersHostError::ByteAccounting)?;
        Ok(())
    }
}

fn seal_call(call: Option<crate::driver::TransactionInitCall>) {
    if let Some(call) = call {
        call.discard_after_driver_shutdown();
    }
}
