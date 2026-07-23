//! Terminal publication, reclamation, and host-failure settlement.

use kafka_client_core::{CreateTopicsInput, CreateTopicsState, DeliveryStatus, Moment};

use crate::completion::{CompletionRegistryError, NotifierJoin, ReclaimStatus};

use super::{CreateTopicsHost, CreateTopicsHostError};

impl CreateTopicsHost {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), CreateTopicsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            let operation_id = operation.machine_id();
            let submission_queued = operation.submission.is_some();
            let input = match operation.machine.state() {
                CreateTopicsState::Ready => CreateTopicsInput::Start {
                    now: Moment::from_tick(u64::MAX),
                },
                CreateTopicsState::AwaitingDriver if submission_queued => {
                    CreateTopicsInput::DriverRejected
                }
                CreateTopicsState::AwaitingDriver => {
                    // Taking the submission crosses the last engine-owned
                    // definitely-unsent boundary. The driver fact may have
                    // been accepted before the host could record it.
                    self.apply(operation_id, CreateTopicsInput::DriverAccepted)?;
                    CreateTopicsInput::TransportFailed {
                        delivery: DeliveryStatus::PossiblySent,
                    }
                }
                CreateTopicsState::Submitted => CreateTopicsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                },
                CreateTopicsState::Completed => {
                    self.publish_terminal(0)?;
                    continue;
                }
            };
            self.apply(operation_id, input)?;
        }
        Ok(())
    }

    pub(crate) fn stop_notifier(&mut self) -> Result<NotifierJoin, CreateTopicsHostError> {
        if !self.operations.is_empty() {
            return Err(CreateTopicsHostError::Unsettled(self.operations.len()));
        }
        self.completions
            .stop_notifier()
            .map_err(CreateTopicsHostError::Completion)
    }

    pub(crate) fn recover_notifier(&mut self) -> Option<NotifierJoin> {
        self.completions.take_notifier()
    }

    pub(crate) fn notifier_thread_id(&self) -> Option<std::thread::ThreadId> {
        self.completions.notifier_thread_id()
    }

    pub(super) fn publish_terminal(&mut self, index: usize) -> Result<(), CreateTopicsHostError> {
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(CreateTopicsHostError::MissingTerminal)?;
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
                Err(CreateTopicsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, CreateTopicsHostError> {
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
            Err(error) => Err(CreateTopicsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), CreateTopicsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(CreateTopicsHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(CreateTopicsHostError::ByteAccounting)?;
        Ok(())
    }
}
