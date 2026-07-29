//! Call polling, route release, publication, reclamation, and recovery.

use kafka_client_core::{
    DeliveryStatus, DescribeReplicaLogDirsInput, DescribeReplicaLogDirsState, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    DescribeReplicaLogDirsHandoff, DescribeReplicaLogDirsHost, DescribeReplicaLogDirsHostError,
    response::terminal_input,
};

impl DescribeReplicaLogDirsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, DescribeReplicaLogDirsHostError> {
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
                .ok_or(DescribeReplicaLogDirsHostError::InvalidHandoff)?;
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
            Err(_) => Err(DescribeReplicaLogDirsHostError::CallCompletion),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DescribeReplicaLogDirsHostError> {
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
                (DescribeReplicaLogDirsState::Ready, _) => self.apply(
                    operation_id,
                    DescribeReplicaLogDirsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    DescribeReplicaLogDirsState::AwaitingDriver,
                    DescribeReplicaLogDirsHandoff::Untouched,
                ) => self.apply(operation_id, DescribeReplicaLogDirsInput::DriverRejected)?,
                (
                    DescribeReplicaLogDirsState::AwaitingDriver,
                    DescribeReplicaLogDirsHandoff::HandedOff,
                ) => {
                    seal_call(self.operations[0].call.take());
                    self.operations[0].current_replicas = None;
                    self.apply(operation_id, DescribeReplicaLogDirsInput::DriverAccepted)?;
                    self.apply(
                        operation_id,
                        DescribeReplicaLogDirsInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (
                    DescribeReplicaLogDirsState::Submitted,
                    DescribeReplicaLogDirsHandoff::Submitted,
                ) => {
                    seal_call(self.operations[0].call.take());
                    self.operations[0].current_replicas = None;
                    self.apply(
                        operation_id,
                        DescribeReplicaLogDirsInput::TransportFailed {
                            delivery: DeliveryStatus::PossiblySent,
                        },
                    )?;
                }
                (DescribeReplicaLogDirsState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => {
                    return Err(DescribeReplicaLogDirsHostError::InvalidHandoff);
                }
            }
        }
        Ok(())
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), DescribeReplicaLogDirsHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(DescribeReplicaLogDirsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(DescribeReplicaLogDirsHostError::MissingTerminal)?;
            let broker_id = operation
                .machine
                .current_broker()
                .ok_or(DescribeReplicaLogDirsHostError::SubmissionMismatch)?;
            let replicas = operation
                .current_replicas
                .as_deref()
                .ok_or(DescribeReplicaLogDirsHostError::MissingReplicas)?;
            terminal_input(raw, broker_id, replicas, operation.remaining_result_bytes)
        };
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(DescribeReplicaLogDirsHostError::ByteAccounting)?;
        self.operations[index].current_replicas = None;
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(DescribeReplicaLogDirsHostError::MissingTerminal)?;
        raw.discard();
        let effect = transition
            .into_effect()
            .ok_or(DescribeReplicaLogDirsHostError::MissingTerminal)?;
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeReplicaLogDirsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].current_replicas.is_some()
        {
            return Err(DescribeReplicaLogDirsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DescribeReplicaLogDirsHostError::MissingTerminal)?;
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
                Err(DescribeReplicaLogDirsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DescribeReplicaLogDirsHostError> {
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
            Err(error) => Err(DescribeReplicaLogDirsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DescribeReplicaLogDirsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(DescribeReplicaLogDirsHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DescribeReplicaLogDirsHostError::ByteAccounting)?;
        Ok(())
    }
}

fn seal_call(call: Option<crate::driver::DescribeReplicaLogDirsCall>) {
    if let Some(call) = call {
        if let Some(recovered) = call.recover_after_driver_shutdown() {
            recovered.seal();
        }
    }
}
