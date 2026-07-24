//! Terminal publication, reclamation, and host-failure settlement.

use kafka_client_core::{CreatePartitionsInput, CreatePartitionsState, DeliveryStatus, Moment};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{CreatePartitionsHost, CreatePartitionsHostError};

impl CreatePartitionsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), CreatePartitionsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            let operation_id = operation.machine_id();
            let submission_queued = operation.submission.is_some();
            let input = match operation.machine.state() {
                CreatePartitionsState::Ready => CreatePartitionsInput::Start {
                    now: Moment::from_tick(u64::MAX),
                },
                CreatePartitionsState::AwaitingDriver if submission_queued => {
                    CreatePartitionsInput::DriverRejected
                }
                CreatePartitionsState::AwaitingDriver => {
                    self.apply(operation_id, CreatePartitionsInput::DriverAccepted)?;
                    CreatePartitionsInput::TransportFailed {
                        delivery: DeliveryStatus::PossiblySent,
                    }
                }
                CreatePartitionsState::Submitted => CreatePartitionsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                },
                CreatePartitionsState::Completed => {
                    self.publish_terminal(0)?;
                    continue;
                }
            };
            self.apply(operation_id, input)?;
        }
        Ok(())
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), CreatePartitionsHostError> {
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(CreatePartitionsHostError::MissingTerminal)?;
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
                Err(CreatePartitionsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, CreatePartitionsHostError> {
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
            Err(error) => Err(CreatePartitionsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), CreatePartitionsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(CreatePartitionsHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(CreatePartitionsHostError::ByteAccounting)?;
        Ok(())
    }
}
