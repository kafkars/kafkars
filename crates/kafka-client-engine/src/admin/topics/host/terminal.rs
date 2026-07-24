//! Terminal publication, reclamation, and host-failure settlement.

use kafka_client_core::{DeliveryStatus, DescribeTopicsInput, DescribeTopicsState, Moment};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{DescribeTopicsHost, DescribeTopicsHostError};

impl DescribeTopicsHost {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), DescribeTopicsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            let operation_id = operation.machine_id();
            let submission_queued = operation.submission.is_some();
            let input = match operation.machine.state() {
                DescribeTopicsState::Ready => DescribeTopicsInput::Start {
                    now: Moment::from_tick(u64::MAX),
                },
                DescribeTopicsState::AwaitingDriver if submission_queued => {
                    DescribeTopicsInput::DriverRejected
                }
                DescribeTopicsState::AwaitingDriver => {
                    // Taking the submission crosses the last engine-owned
                    // definitely-unsent boundary. The driver fact may have
                    // been accepted before the host could record it.
                    self.apply(operation_id, DescribeTopicsInput::DriverAccepted)?;
                    DescribeTopicsInput::TransportFailed {
                        delivery: DeliveryStatus::PossiblySent,
                    }
                }
                DescribeTopicsState::Submitted => DescribeTopicsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                },
                DescribeTopicsState::Completed => {
                    self.publish_terminal(0)?;
                    continue;
                }
            };
            self.apply(operation_id, input)?;
        }
        Ok(())
    }

    pub(super) fn publish_terminal(&mut self, index: usize) -> Result<(), DescribeTopicsHostError> {
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DescribeTopicsHostError::MissingTerminal)?;
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
                Err(DescribeTopicsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DescribeTopicsHostError> {
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
            Err(error) => Err(DescribeTopicsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DescribeTopicsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(DescribeTopicsHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DescribeTopicsHostError::ByteAccounting)?;
        Ok(())
    }
}
