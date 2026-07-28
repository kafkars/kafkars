//! Call polling, route release, publication, reclamation, and recovery.

use kafka_client_core::{
    AdminListConsumerGroupsInput, AdminListConsumerGroupsState, DeliveryStatus, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    ListConsumerGroupsHandoff, ListConsumerGroupsHost, ListConsumerGroupsHostError,
    response::terminal_input,
};

impl ListConsumerGroupsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, ListConsumerGroupsHostError> {
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
                .ok_or(ListConsumerGroupsHostError::InvalidHandoff)?;
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
            Err(_) => Err(ListConsumerGroupsHostError::CallCompletion),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ListConsumerGroupsHostError> {
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
                (AdminListConsumerGroupsState::Ready, _) => self.apply(
                    operation_id,
                    AdminListConsumerGroupsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    AdminListConsumerGroupsState::AwaitingDiscoveryDriver
                    | AdminListConsumerGroupsState::AwaitingBrokerDriver,
                    ListConsumerGroupsHandoff::Untouched,
                ) => self.apply(operation_id, AdminListConsumerGroupsInput::DriverRejected)?,
                (
                    AdminListConsumerGroupsState::AwaitingDiscoveryDriver
                    | AdminListConsumerGroupsState::AwaitingBrokerDriver,
                    ListConsumerGroupsHandoff::HandedOff,
                ) => {
                    seal_call(self.operations[0].call.take());
                    self.apply(operation_id, AdminListConsumerGroupsInput::DriverAccepted)?;
                    self.apply(
                        operation_id,
                        AdminListConsumerGroupsInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (
                    AdminListConsumerGroupsState::DiscoverySubmitted
                    | AdminListConsumerGroupsState::BrokerSubmitted,
                    ListConsumerGroupsHandoff::Submitted,
                ) => {
                    seal_call(self.operations[0].call.take());
                    self.apply(
                        operation_id,
                        AdminListConsumerGroupsInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (AdminListConsumerGroupsState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(ListConsumerGroupsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), ListConsumerGroupsHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(ListConsumerGroupsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(ListConsumerGroupsHostError::MissingTerminal)?;
            terminal_input(raw, operation.remaining_result_bytes)
        };
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(ListConsumerGroupsHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(ListConsumerGroupsHostError::MissingTerminal)?;
        raw.discard();
        let effect = transition
            .into_effect()
            .ok_or(ListConsumerGroupsHostError::MissingTerminal)?;
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), ListConsumerGroupsHostError> {
        if self.operations[index].call.is_some() || self.operations[index].raw_terminal.is_some() {
            return Err(ListConsumerGroupsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(ListConsumerGroupsHostError::MissingTerminal)?;
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
                Err(ListConsumerGroupsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, ListConsumerGroupsHostError> {
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
            Err(error) => Err(ListConsumerGroupsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), ListConsumerGroupsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(ListConsumerGroupsHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(ListConsumerGroupsHostError::ByteAccounting)?;
        Ok(())
    }
}

fn seal_call(call: Option<crate::driver::ListConsumerGroupsCall>) {
    if let Some(call) = call {
        if let Some(recovered) = call.recover_after_driver_shutdown() {
            recovered.seal();
        }
    }
}
