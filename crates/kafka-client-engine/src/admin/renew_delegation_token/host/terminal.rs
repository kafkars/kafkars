//! Call polling, route release, publication, reclamation, and recovery.

use kafka_client_core::{
    DeliveryStatus, Moment, RenewDelegationTokenEffect, RenewDelegationTokenInput,
    RenewDelegationTokenState,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    RenewDelegationTokenHandoff, RenewDelegationTokenHost, RenewDelegationTokenHostError,
    RenewDelegationTokenOperation, response::terminal_input,
};

impl RenewDelegationTokenHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, RenewDelegationTokenHostError> {
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.call.is_some())
        else {
            return Ok(false);
        };
        let Some(terminal) = poll_call(&mut self.operations[index])? else {
            return Ok(false);
        };
        store_raw_terminal(&mut self.operations[index], terminal);
        self.settle_raw(index)?;
        Ok(true)
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), RenewDelegationTokenHostError> {
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
                (RenewDelegationTokenState::Ready, _) => self.apply(
                    operation_id,
                    RenewDelegationTokenInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    RenewDelegationTokenState::AwaitingDriver,
                    RenewDelegationTokenHandoff::Untouched,
                ) => self.apply(operation_id, RenewDelegationTokenInput::DriverRejected)?,
                (
                    RenewDelegationTokenState::AwaitingDriver,
                    RenewDelegationTokenHandoff::HandedOff,
                ) => {
                    seal_call(take_call(&mut self.operations[0]));
                    self.apply(operation_id, RenewDelegationTokenInput::DriverAccepted)?;
                    self.apply(
                        operation_id,
                        RenewDelegationTokenInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (RenewDelegationTokenState::Submitted, RenewDelegationTokenHandoff::Submitted) => {
                    seal_call(take_call(&mut self.operations[0]));
                    self.apply(
                        operation_id,
                        RenewDelegationTokenInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (RenewDelegationTokenState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(RenewDelegationTokenHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), RenewDelegationTokenHostError> {
        let operation = self
            .operations
            .get_mut(index)
            .ok_or(RenewDelegationTokenHostError::UnknownOperation)?;
        settle_operation(operation)?;
        self.publish_terminal(index)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), RenewDelegationTokenHostError> {
        let (completion_id, terminal) = take_publishable_terminal(&mut self.operations[index])?;
        match self.completions.publish(completion_id, terminal) {
            Ok(()) => {
                let operation = self.operations.remove(index);
                self.published_bytes
                    .push((operation.completion_id, operation.retained_bytes));
                Ok(())
            }
            Err((error, terminal)) => {
                restore_terminal(&mut self.operations[index], terminal);
                Err(RenewDelegationTokenHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, RenewDelegationTokenHostError> {
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
            Err(error) => Err(RenewDelegationTokenHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), RenewDelegationTokenHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(RenewDelegationTokenHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(RenewDelegationTokenHostError::ByteAccounting)?;
        Ok(())
    }
}

fn poll_call(
    operation: &mut RenewDelegationTokenOperation,
) -> Result<Option<crate::driver::RenewDelegationTokenRawTerminal>, RenewDelegationTokenHostError> {
    let terminal = operation
        .call
        .as_mut()
        .ok_or(RenewDelegationTokenHostError::InvalidHandoff)?
        .try_terminal();
    let Some(terminal) = terminal else {
        return Ok(None);
    };
    drop(operation.call.take());
    terminal
        .map(Some)
        .map_err(|_error| RenewDelegationTokenHostError::CallCompletion)
}

fn store_raw_terminal(
    operation: &mut RenewDelegationTokenOperation,
    terminal: crate::driver::RenewDelegationTokenRawTerminal,
) {
    operation.raw_terminal = Some(terminal);
}

fn take_call(
    operation: &mut RenewDelegationTokenOperation,
) -> Option<crate::driver::RenewDelegationTokenCall> {
    operation.call.take()
}

fn settle_operation(
    operation: &mut RenewDelegationTokenOperation,
) -> Result<(), RenewDelegationTokenHostError> {
    let raw = operation
        .raw_terminal
        .as_ref()
        .ok_or(RenewDelegationTokenHostError::MissingTerminal)?;
    let (input, retained_bytes) = terminal_input(raw, operation.remaining_result_bytes);
    operation.remaining_result_bytes = operation
        .remaining_result_bytes
        .checked_sub(retained_bytes)
        .ok_or(RenewDelegationTokenHostError::ByteAccounting)?;
    let machine = &mut operation.machine;
    let transition = machine.apply(input)?;
    let raw = operation
        .raw_terminal
        .take()
        .ok_or(RenewDelegationTokenHostError::MissingTerminal)?;
    raw.discard();
    match transition.into_effect() {
        Some(RenewDelegationTokenEffect::Complete {
            operation_id,
            terminal,
        }) if operation_id == operation.operation_id => {
            operation.terminal = Some(terminal);
            Ok(())
        }
        _ => Err(RenewDelegationTokenHostError::MissingTerminal),
    }
}

fn take_publishable_terminal(
    operation: &mut RenewDelegationTokenOperation,
) -> Result<
    (
        crate::completion::CompletionId,
        kafka_client_core::RenewDelegationTokenTerminal,
    ),
    RenewDelegationTokenHostError,
> {
    if operation.call.is_some() || operation.raw_terminal.is_some() {
        return Err(RenewDelegationTokenHostError::InvalidHandoff);
    }
    let terminal = operation
        .terminal
        .take()
        .ok_or(RenewDelegationTokenHostError::MissingTerminal)?;
    Ok((operation.completion_id, terminal))
}

fn restore_terminal(
    operation: &mut RenewDelegationTokenOperation,
    terminal: kafka_client_core::RenewDelegationTokenTerminal,
) {
    operation.terminal = Some(terminal);
}

fn seal_call(call: Option<crate::driver::RenewDelegationTokenCall>) {
    if let Some(call) = call
        && let Some(recovered) = call.recover_after_driver_shutdown()
    {
        seal_recovered_call(recovered);
    }
}

fn seal_recovered_call(recovered: crate::driver::RecoveredRenewDelegationTokenCall) {
    recovered.seal();
}
