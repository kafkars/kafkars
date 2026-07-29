//! Call polling, route release, publication, reclamation, and recovery.

use kafka_client_core::{
    DeliveryStatus, DescribeDelegationTokensEffect, DescribeDelegationTokensInput,
    DescribeDelegationTokensState, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    DescribeDelegationTokensHandoff, DescribeDelegationTokensHost,
    DescribeDelegationTokensHostError, DescribeDelegationTokensOperation, response::terminal_input,
};

impl DescribeDelegationTokensOperation {
    fn poll_call(&mut self) -> Result<bool, DescribeDelegationTokensHostError> {
        let terminal = self
            .call
            .as_mut()
            .ok_or(DescribeDelegationTokensHostError::InvalidHandoff)?
            .try_terminal();
        let Some(terminal) = terminal else {
            return Ok(false);
        };
        drop(self.call.take());
        match terminal {
            Ok(terminal) => {
                self.raw_terminal = Some(terminal);
                Ok(true)
            }
            Err(_error) => Err(DescribeDelegationTokensHostError::CallCompletion),
        }
    }

    fn take_call_for_recovery(&mut self) -> Option<crate::driver::DescribeDelegationTokensCall> {
        self.call.take()
    }

    fn settle_raw(&mut self) -> Result<(), DescribeDelegationTokensHostError> {
        let raw = self
            .raw_terminal
            .as_ref()
            .ok_or(DescribeDelegationTokensHostError::MissingTerminal)?;
        let (input, retained_bytes) =
            terminal_input(raw, &self.correlation_plan, self.remaining_result_bytes);
        self.remaining_result_bytes = self
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(DescribeDelegationTokensHostError::ByteAccounting)?;
        let transition = self.machine.apply(input)?;
        let raw = self
            .raw_terminal
            .take()
            .ok_or(DescribeDelegationTokensHostError::MissingTerminal)?;
        raw.discard();
        match transition.into_effect() {
            Some(DescribeDelegationTokensEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operation_id => {
                self.terminal = Some(terminal);
                Ok(())
            }
            _ => Err(DescribeDelegationTokensHostError::MissingTerminal),
        }
    }
}

impl DescribeDelegationTokensHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, DescribeDelegationTokensHostError> {
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.call.is_some())
        else {
            return Ok(false);
        };
        if !self.operations[index].poll_call()? {
            return Ok(false);
        }
        self.settle_raw(index)?;
        Ok(true)
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DescribeDelegationTokensHostError> {
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
                (DescribeDelegationTokensState::Ready, _) => self.apply(
                    operation_id,
                    DescribeDelegationTokensInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    DescribeDelegationTokensState::AwaitingDriver,
                    DescribeDelegationTokensHandoff::Untouched,
                ) => self.apply(operation_id, DescribeDelegationTokensInput::DriverRejected)?,
                (
                    DescribeDelegationTokensState::AwaitingDriver,
                    DescribeDelegationTokensHandoff::HandedOff,
                ) => {
                    seal_call(self.operations[0].take_call_for_recovery());
                    self.apply(operation_id, DescribeDelegationTokensInput::DriverAccepted)?;
                    self.apply(
                        operation_id,
                        DescribeDelegationTokensInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (
                    DescribeDelegationTokensState::Submitted,
                    DescribeDelegationTokensHandoff::Submitted,
                ) => {
                    seal_call(self.operations[0].take_call_for_recovery());
                    self.apply(
                        operation_id,
                        DescribeDelegationTokensInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (DescribeDelegationTokensState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(DescribeDelegationTokensHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), DescribeDelegationTokensHostError> {
        let operation = self
            .operations
            .get_mut(index)
            .ok_or(DescribeDelegationTokensHostError::UnknownOperation)?;
        operation.settle_raw()?;
        self.publish_terminal(index)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeDelegationTokensHostError> {
        if self.operations[index].call.is_some() || self.operations[index].raw_terminal.is_some() {
            return Err(DescribeDelegationTokensHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DescribeDelegationTokensHostError::MissingTerminal)?;
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
                Err(DescribeDelegationTokensHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DescribeDelegationTokensHostError> {
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
            Err(error) => Err(DescribeDelegationTokensHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DescribeDelegationTokensHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(DescribeDelegationTokensHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DescribeDelegationTokensHostError::ByteAccounting)?;
        Ok(())
    }
}

fn seal_call(call: Option<crate::driver::DescribeDelegationTokensCall>) {
    if let Some(call) = call
        && let Some(recovered) = call.recover_after_driver_shutdown()
    {
        seal_recovered_call(recovered);
    }
}

fn seal_recovered_call(recovered: crate::driver::RecoveredDescribeDelegationTokensCall) {
    recovered.seal();
}
