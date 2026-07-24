//! Terminal publication, reclamation, and host-failure settlement ownership.

use kafka_client_core::{DeliveryStatus, DescribeConfigsInput, DescribeConfigsState, Moment};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{DescribeConfigsHost, DescribeConfigsHostError};

impl DescribeConfigsHost {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), DescribeConfigsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            let operation_id = operation.machine_id();
            let submission_queued = operation.submission.is_some();
            let input = match operation.machine.state() {
                DescribeConfigsState::Ready => DescribeConfigsInput::Start {
                    now: Moment::from_tick(u64::MAX),
                },
                DescribeConfigsState::AwaitingDriver if submission_queued => {
                    DescribeConfigsInput::DriverRejected
                }
                DescribeConfigsState::AwaitingDriver => {
                    self.apply(operation_id, DescribeConfigsInput::DriverAccepted)?;
                    DescribeConfigsInput::TransportFailed {
                        delivery: DeliveryStatus::PossiblySent,
                    }
                }
                DescribeConfigsState::Submitted => DescribeConfigsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                },
                DescribeConfigsState::Completed => {
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
    ) -> Result<(), DescribeConfigsHostError> {
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DescribeConfigsHostError::MissingTerminal)?;
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
                Err(DescribeConfigsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DescribeConfigsHostError> {
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
            Err(error) => Err(DescribeConfigsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DescribeConfigsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(DescribeConfigsHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DescribeConfigsHostError::ByteAccounting)?;
        Ok(())
    }
}
