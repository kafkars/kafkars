//! Atomic Admin `DeleteRecords` transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    DeleteRecordsBatch, DeleteRecordsEffect, DeleteRecordsFailure, DeleteRecordsFailureKind,
    DeleteRecordsInput, DeleteRecordsMachine, DeleteRecordsMachineError, DeleteRecordsState,
    DeleteRecordsTerminal, DeleteRecordsTransition,
};

impl DeleteRecordsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DeleteRecordsInput,
    ) -> Result<DeleteRecordsTransition, DeleteRecordsMachineError> {
        if self.state == DeleteRecordsState::Completed {
            return Err(DeleteRecordsMachineError::AlreadyCompleted);
        }
        match input {
            DeleteRecordsInput::Start { now } => self.start(now),
            DeleteRecordsInput::DriverAccepted => self.driver_accepted(),
            DeleteRecordsInput::DriverRejected => self.finish_awaiting(
                DeleteRecordsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DeleteRecordsInput::DeadlineElapsed => self.finish_awaiting(
                DeleteRecordsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DeleteRecordsInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(DeleteRecordsFailureKind::DeadlineElapsed, delivery)
            }
            DeleteRecordsInput::BrokerResponded {
                throttle_time_ms,
                outcome,
            } => self.broker_responded(throttle_time_ms, outcome),
            DeleteRecordsInput::ResponseTooLarge => self.finish_submitted(
                DeleteRecordsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DeleteRecordsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(DeleteRecordsFailureKind::Compatibility, delivery)
            }
            DeleteRecordsInput::TransportFailed { delivery } => {
                self.finish_submitted(DeleteRecordsFailureKind::Transport, delivery)
            }
            DeleteRecordsInput::InvalidResponse => self.finish_submitted(
                DeleteRecordsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DeleteRecordsTransition, DeleteRecordsMachineError> {
        if self.state != DeleteRecordsState::Ready {
            return Err(DeleteRecordsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return self.finish_failure(
                DeleteRecordsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            );
        }
        self.submit_current()
    }

    fn submit_current(&mut self) -> Result<DeleteRecordsTransition, DeleteRecordsMachineError> {
        let Some(target) = self.plan.targets().get(self.next_target).cloned() else {
            return Err(DeleteRecordsMachineError::InvalidState);
        };
        self.state = DeleteRecordsState::AwaitingDriver;
        Ok(DeleteRecordsTransition::one(DeleteRecordsEffect::Submit {
            operation_id: self.operation_id,
            deadline: self.deadline,
            target,
        }))
    }

    fn driver_accepted(&mut self) -> Result<DeleteRecordsTransition, DeleteRecordsMachineError> {
        if self.state != DeleteRecordsState::AwaitingDriver {
            return Err(DeleteRecordsMachineError::InvalidState);
        }
        self.state = DeleteRecordsState::Submitted;
        Ok(DeleteRecordsTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        outcome: super::DeleteRecordsOutcome,
    ) -> Result<DeleteRecordsTransition, DeleteRecordsMachineError> {
        if self.state != DeleteRecordsState::Submitted {
            return Err(DeleteRecordsMachineError::InvalidState);
        }
        let Some(target) = self.plan.targets().get(self.next_target) else {
            return Err(DeleteRecordsMachineError::InvalidState);
        };
        if target.topic() != outcome.topic() || target.partition() != outcome.partition() {
            return self.finish_failure(
                DeleteRecordsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            );
        }
        self.outcomes.push(outcome);
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.next_target += 1;
        if self.next_target == self.plan.targets().len() {
            let outcomes = core::mem::take(&mut self.outcomes);
            return Ok(
                self.finish(DeleteRecordsTerminal::Deleted(DeleteRecordsBatch::new(
                    self.maximum_throttle_time_ms,
                    outcomes,
                ))),
            );
        }
        self.submit_current()
    }

    fn finish_awaiting(
        &mut self,
        kind: DeleteRecordsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteRecordsTransition, DeleteRecordsMachineError> {
        if self.state != DeleteRecordsState::AwaitingDriver {
            return Err(DeleteRecordsMachineError::InvalidState);
        }
        self.finish_failure(kind, delivery)
    }

    fn finish_submitted(
        &mut self,
        kind: DeleteRecordsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteRecordsTransition, DeleteRecordsMachineError> {
        if self.state != DeleteRecordsState::Submitted {
            return Err(DeleteRecordsMachineError::InvalidState);
        }
        self.finish_failure(kind, delivery)
    }

    fn finish_failure(
        &mut self,
        kind: DeleteRecordsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteRecordsTransition, DeleteRecordsMachineError> {
        let Some(failed_target) = self.plan.targets().get(self.next_target).cloned() else {
            return Err(DeleteRecordsMachineError::InvalidState);
        };
        let completed = core::mem::take(&mut self.outcomes);
        let unattempted = self
            .plan
            .targets()
            .get(self.next_target.saturating_add(1)..)
            .unwrap_or_default()
            .to_vec();
        Ok(
            self.finish(DeleteRecordsTerminal::Failed(DeleteRecordsFailure::new(
                kind,
                delivery,
                self.maximum_throttle_time_ms,
                completed,
                failed_target,
                unattempted,
            ))),
        )
    }

    fn finish(&mut self, terminal: DeleteRecordsTerminal) -> DeleteRecordsTransition {
        self.state = DeleteRecordsState::Completed;
        DeleteRecordsTransition::one(DeleteRecordsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
