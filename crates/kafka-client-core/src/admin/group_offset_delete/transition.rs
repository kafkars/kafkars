//! Atomic consumer-group offset deletion transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    DeleteConsumerGroupOffsetOutcome, DeleteConsumerGroupOffsetsBatch,
    DeleteConsumerGroupOffsetsEffect, DeleteConsumerGroupOffsetsFailure,
    DeleteConsumerGroupOffsetsFailureKind, DeleteConsumerGroupOffsetsInput,
    DeleteConsumerGroupOffsetsMachine, DeleteConsumerGroupOffsetsMachineError,
    DeleteConsumerGroupOffsetsState, DeleteConsumerGroupOffsetsTerminal,
    DeleteConsumerGroupOffsetsTransition,
};

impl DeleteConsumerGroupOffsetsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DeleteConsumerGroupOffsetsInput,
    ) -> Result<DeleteConsumerGroupOffsetsTransition, DeleteConsumerGroupOffsetsMachineError> {
        if self.state == DeleteConsumerGroupOffsetsState::Completed {
            return Err(DeleteConsumerGroupOffsetsMachineError::AlreadyCompleted);
        }
        match input {
            DeleteConsumerGroupOffsetsInput::Start { now } => self.start(now),
            DeleteConsumerGroupOffsetsInput::DriverAccepted => self.driver_accepted(),
            DeleteConsumerGroupOffsetsInput::DriverRejected => self.finish_awaiting(
                DeleteConsumerGroupOffsetsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DeleteConsumerGroupOffsetsInput::DeadlineElapsed => self.finish_awaiting(
                DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DeleteConsumerGroupOffsetsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed,
                    delivery,
                ),
            DeleteConsumerGroupOffsetsInput::BrokerResponded { batch } => {
                self.broker_responded(batch)
            }
            DeleteConsumerGroupOffsetsInput::BrokerRejected { code } => self.finish_submitted(
                DeleteConsumerGroupOffsetsFailureKind::Broker(code),
                DeliveryStatus::PossiblySent,
            ),
            DeleteConsumerGroupOffsetsInput::ResponseTooLarge => self.finish_submitted(
                DeleteConsumerGroupOffsetsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DeleteConsumerGroupOffsetsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    DeleteConsumerGroupOffsetsFailureKind::Compatibility,
                    delivery,
                ),
            DeleteConsumerGroupOffsetsInput::TransportFailed { delivery } => {
                self.finish_submitted(DeleteConsumerGroupOffsetsFailureKind::Transport, delivery)
            }
            DeleteConsumerGroupOffsetsInput::InvalidResponse => self.finish_submitted(
                DeleteConsumerGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DeleteConsumerGroupOffsetsTransition, DeleteConsumerGroupOffsetsMachineError> {
        if self.state != DeleteConsumerGroupOffsetsState::Ready {
            return Err(DeleteConsumerGroupOffsetsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = DeleteConsumerGroupOffsetsState::AwaitingDriver;
        Ok(DeleteConsumerGroupOffsetsTransition::one(
            DeleteConsumerGroupOffsetsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DeleteConsumerGroupOffsetsTransition, DeleteConsumerGroupOffsetsMachineError> {
        if self.state != DeleteConsumerGroupOffsetsState::AwaitingDriver {
            return Err(DeleteConsumerGroupOffsetsMachineError::InvalidState);
        }
        self.state = DeleteConsumerGroupOffsetsState::Submitted;
        Ok(DeleteConsumerGroupOffsetsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: DeleteConsumerGroupOffsetsBatch,
    ) -> Result<DeleteConsumerGroupOffsetsTransition, DeleteConsumerGroupOffsetsMachineError> {
        if self.state != DeleteConsumerGroupOffsetsState::Submitted {
            return Err(DeleteConsumerGroupOffsetsMachineError::InvalidState);
        }
        if !self.outcomes_match_plan(batch.outcomes()) {
            return Ok(self.finish_failure(
                DeleteConsumerGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        Ok(self.finish(DeleteConsumerGroupOffsetsTerminal::Deleted(batch)))
    }

    fn outcomes_match_plan(&self, outcomes: &[DeleteConsumerGroupOffsetOutcome]) -> bool {
        self.plan.targets().len() == outcomes.len()
            && self
                .plan
                .targets()
                .iter()
                .zip(outcomes)
                .all(|(target, outcome)| {
                    target.topic() == outcome.topic() && target.partition() == outcome.partition()
                })
    }

    fn finish_awaiting(
        &mut self,
        kind: DeleteConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteConsumerGroupOffsetsTransition, DeleteConsumerGroupOffsetsMachineError> {
        if self.state != DeleteConsumerGroupOffsetsState::AwaitingDriver {
            return Err(DeleteConsumerGroupOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: DeleteConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteConsumerGroupOffsetsTransition, DeleteConsumerGroupOffsetsMachineError> {
        if self.state != DeleteConsumerGroupOffsetsState::Submitted {
            return Err(DeleteConsumerGroupOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: DeleteConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> DeleteConsumerGroupOffsetsTransition {
        self.finish(DeleteConsumerGroupOffsetsTerminal::Failed(
            DeleteConsumerGroupOffsetsFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: DeleteConsumerGroupOffsetsTerminal,
    ) -> DeleteConsumerGroupOffsetsTransition {
        self.state = DeleteConsumerGroupOffsetsState::Completed;
        DeleteConsumerGroupOffsetsTransition::one(DeleteConsumerGroupOffsetsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
