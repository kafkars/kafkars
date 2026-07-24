//! Terminal publication, observer reclamation, and recovery-certainty ownership.

use kafka_client_core::{
    DeliveryStatus, IncrementalAlterConfigsInput, IncrementalAlterConfigsState, Moment,
};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    IncrementalAlterConfigsHandoff, IncrementalAlterConfigsHost, IncrementalAlterConfigsHostError,
};

impl IncrementalAlterConfigsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), IncrementalAlterConfigsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            let operation_id = operation.machine_id();
            let input = match (operation.machine.state(), operation.handoff) {
                (IncrementalAlterConfigsState::Ready, _) => IncrementalAlterConfigsInput::Start {
                    now: Moment::from_tick(u64::MAX),
                },
                (
                    IncrementalAlterConfigsState::AwaitingDriver,
                    IncrementalAlterConfigsHandoff::Untouched,
                ) => IncrementalAlterConfigsInput::DriverRejected,
                (
                    IncrementalAlterConfigsState::AwaitingDriver,
                    IncrementalAlterConfigsHandoff::HandedOff,
                ) => {
                    self.apply(operation_id, IncrementalAlterConfigsInput::DriverAccepted)?;
                    IncrementalAlterConfigsInput::TransportFailed {
                        delivery: DeliveryStatus::PossiblySent,
                    }
                }
                (
                    IncrementalAlterConfigsState::Submitted,
                    IncrementalAlterConfigsHandoff::Submitted,
                ) => IncrementalAlterConfigsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                },
                (IncrementalAlterConfigsState::Completed, _) => {
                    self.publish_terminal(0)?;
                    continue;
                }
                _ => return Err(IncrementalAlterConfigsHostError::InvalidHandoff),
            };
            self.apply(operation_id, input)?;
        }
        Ok(())
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), IncrementalAlterConfigsHostError> {
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(IncrementalAlterConfigsHostError::MissingTerminal)?;
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
                Err(IncrementalAlterConfigsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, IncrementalAlterConfigsHostError> {
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
            Err(error) => Err(IncrementalAlterConfigsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), IncrementalAlterConfigsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(IncrementalAlterConfigsHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(IncrementalAlterConfigsHostError::ByteAccounting)?;
        Ok(())
    }
}
