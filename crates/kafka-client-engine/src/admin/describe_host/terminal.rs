//! Terminal publication, reclamation, and failure settlement.

use kafka_client_core::{DeliveryStatus, DescribeClusterInput, DescribeClusterState, Moment};

use crate::completion::{CompletionRegistryError, NotifierJoin, ReclaimStatus};

use super::{DescribeClusterHost, DescribeClusterHostError};

impl DescribeClusterHost {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), DescribeClusterHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            let operation_id = operation.operation_id;
            let submission_queued = operation.submission.is_some();
            let input = match operation.machine.state() {
                DescribeClusterState::Ready => DescribeClusterInput::Start {
                    now: Moment::from_tick(u64::MAX),
                },
                DescribeClusterState::AwaitingDriver if submission_queued => {
                    DescribeClusterInput::DriverRejected
                }
                DescribeClusterState::AwaitingDriver => {
                    self.apply(operation_id, DescribeClusterInput::DriverAccepted)?;
                    DescribeClusterInput::TransportFailed {
                        delivery: DeliveryStatus::PossiblySent,
                    }
                }
                DescribeClusterState::Submitted => DescribeClusterInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                },
                DescribeClusterState::Completed => {
                    self.publish_terminal(0)?;
                    continue;
                }
            };
            self.apply(operation_id, input)?;
        }
        Ok(())
    }

    pub(crate) fn stop_notifier(&mut self) -> Result<NotifierJoin, DescribeClusterHostError> {
        if !self.operations.is_empty() {
            return Err(DescribeClusterHostError::Unsettled(self.operations.len()));
        }
        self.completions
            .stop_notifier()
            .map_err(DescribeClusterHostError::Completion)
    }

    pub(crate) fn recover_notifier(&mut self) -> Option<NotifierJoin> {
        self.completions.take_notifier()
    }

    pub(crate) fn notifier_thread_id(&self) -> Option<std::thread::ThreadId> {
        self.completions.notifier_thread_id()
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeClusterHostError> {
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(DescribeClusterHostError::MissingTerminal)?;
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
                Err(DescribeClusterHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, DescribeClusterHostError> {
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
            Err(error) => Err(DescribeClusterHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), DescribeClusterHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _bytes)| *id == completion_id)
            .ok_or(DescribeClusterHostError::ByteAccounting)?;
        let (_id, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(DescribeClusterHostError::ByteAccounting)?;
        Ok(())
    }
}
