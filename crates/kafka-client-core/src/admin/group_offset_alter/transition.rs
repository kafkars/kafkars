//! Atomic consumer-group offset alteration transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    AlterConsumerGroupOffsetOutcome, AlterConsumerGroupOffsetsBatch,
    AlterConsumerGroupOffsetsEffect, AlterConsumerGroupOffsetsFailure,
    AlterConsumerGroupOffsetsFailureKind, AlterConsumerGroupOffsetsInput,
    AlterConsumerGroupOffsetsMachine, AlterConsumerGroupOffsetsMachineError,
    AlterConsumerGroupOffsetsState, AlterConsumerGroupOffsetsTerminal,
    AlterConsumerGroupOffsetsTransition,
};

impl AlterConsumerGroupOffsetsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AlterConsumerGroupOffsetsInput,
    ) -> Result<AlterConsumerGroupOffsetsTransition, AlterConsumerGroupOffsetsMachineError> {
        if self.state == AlterConsumerGroupOffsetsState::Completed {
            return Err(AlterConsumerGroupOffsetsMachineError::AlreadyCompleted);
        }
        match input {
            AlterConsumerGroupOffsetsInput::Start { now } => self.start(now),
            AlterConsumerGroupOffsetsInput::DriverAccepted => self.driver_accepted(),
            AlterConsumerGroupOffsetsInput::DriverRejected => self.finish_awaiting(
                AlterConsumerGroupOffsetsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            AlterConsumerGroupOffsetsInput::DeadlineElapsed => self.finish_awaiting(
                AlterConsumerGroupOffsetsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            AlterConsumerGroupOffsetsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    AlterConsumerGroupOffsetsFailureKind::DeadlineElapsed,
                    delivery,
                ),
            AlterConsumerGroupOffsetsInput::BrokerResponded { batch } => {
                self.broker_responded(batch)
            }
            AlterConsumerGroupOffsetsInput::ResponseTooLarge => self.finish_submitted(
                AlterConsumerGroupOffsetsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AlterConsumerGroupOffsetsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    AlterConsumerGroupOffsetsFailureKind::Compatibility,
                    delivery,
                ),
            AlterConsumerGroupOffsetsInput::TransportFailed { delivery } => {
                self.finish_submitted(AlterConsumerGroupOffsetsFailureKind::Transport, delivery)
            }
            AlterConsumerGroupOffsetsInput::InvalidResponse => self.finish_submitted(
                AlterConsumerGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AlterConsumerGroupOffsetsTransition, AlterConsumerGroupOffsetsMachineError> {
        if self.state != AlterConsumerGroupOffsetsState::Ready {
            return Err(AlterConsumerGroupOffsetsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AlterConsumerGroupOffsetsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = AlterConsumerGroupOffsetsState::AwaitingDriver;
        Ok(AlterConsumerGroupOffsetsTransition::one(
            AlterConsumerGroupOffsetsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AlterConsumerGroupOffsetsTransition, AlterConsumerGroupOffsetsMachineError> {
        if self.state != AlterConsumerGroupOffsetsState::AwaitingDriver {
            return Err(AlterConsumerGroupOffsetsMachineError::InvalidState);
        }
        self.state = AlterConsumerGroupOffsetsState::Submitted;
        Ok(AlterConsumerGroupOffsetsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: AlterConsumerGroupOffsetsBatch,
    ) -> Result<AlterConsumerGroupOffsetsTransition, AlterConsumerGroupOffsetsMachineError> {
        if self.state != AlterConsumerGroupOffsetsState::Submitted {
            return Err(AlterConsumerGroupOffsetsMachineError::InvalidState);
        }
        if !self.outcomes_match_plan(batch.outcomes()) {
            return Ok(self.finish_failure(
                AlterConsumerGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        Ok(self.finish(AlterConsumerGroupOffsetsTerminal::Altered(batch)))
    }

    fn outcomes_match_plan(&self, outcomes: &[AlterConsumerGroupOffsetOutcome]) -> bool {
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
        kind: AlterConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterConsumerGroupOffsetsTransition, AlterConsumerGroupOffsetsMachineError> {
        if self.state != AlterConsumerGroupOffsetsState::AwaitingDriver {
            return Err(AlterConsumerGroupOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: AlterConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterConsumerGroupOffsetsTransition, AlterConsumerGroupOffsetsMachineError> {
        if self.state != AlterConsumerGroupOffsetsState::Submitted {
            return Err(AlterConsumerGroupOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: AlterConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> AlterConsumerGroupOffsetsTransition {
        self.finish(AlterConsumerGroupOffsetsTerminal::Failed(
            AlterConsumerGroupOffsetsFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: AlterConsumerGroupOffsetsTerminal,
    ) -> AlterConsumerGroupOffsetsTransition {
        self.state = AlterConsumerGroupOffsetsState::Completed;
        AlterConsumerGroupOffsetsTransition::one(AlterConsumerGroupOffsetsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
