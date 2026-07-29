//! Atomic API-91 transitions, bounded correlation, and terminal assignment.

use crate::DeliveryStatus;

use super::{
    AlterShareGroupOffsetsBatch, AlterShareGroupOffsetsBrokerError, AlterShareGroupOffsetsEffect,
    AlterShareGroupOffsetsFailure, AlterShareGroupOffsetsFailureKind, AlterShareGroupOffsetsInput,
    AlterShareGroupOffsetsMachine, AlterShareGroupOffsetsMachineError, AlterShareGroupOffsetsState,
    AlterShareGroupOffsetsTerminal, AlterShareGroupOffsetsTransition,
};

use super::correlation::{ResponseValidation, broker_error_is_valid, correlate_response};

impl AlterShareGroupOffsetsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AlterShareGroupOffsetsInput,
    ) -> Result<AlterShareGroupOffsetsTransition, AlterShareGroupOffsetsMachineError> {
        if self.state == AlterShareGroupOffsetsState::Completed {
            return Err(AlterShareGroupOffsetsMachineError::AlreadyCompleted);
        }
        match input {
            AlterShareGroupOffsetsInput::Start { now } => self.start(now),
            AlterShareGroupOffsetsInput::DriverAccepted => self.driver_accepted(),
            AlterShareGroupOffsetsInput::DriverRejected => self.finish_awaiting(
                AlterShareGroupOffsetsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            AlterShareGroupOffsetsInput::DeadlineElapsed => self.finish_awaiting(
                AlterShareGroupOffsetsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            AlterShareGroupOffsetsInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(AlterShareGroupOffsetsFailureKind::DeadlineElapsed, delivery)
            }
            AlterShareGroupOffsetsInput::BrokerResponded { batch } => self.broker_responded(batch),
            AlterShareGroupOffsetsInput::BrokerRejected { error } => self.broker_rejected(error),
            AlterShareGroupOffsetsInput::ResponseTooLarge => self.finish_submitted(
                AlterShareGroupOffsetsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AlterShareGroupOffsetsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(AlterShareGroupOffsetsFailureKind::Compatibility, delivery)
            }
            AlterShareGroupOffsetsInput::TransportFailed { delivery } => {
                self.finish_submitted(AlterShareGroupOffsetsFailureKind::Transport, delivery)
            }
            AlterShareGroupOffsetsInput::InvalidResponse => self.finish_submitted(
                AlterShareGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AlterShareGroupOffsetsTransition, AlterShareGroupOffsetsMachineError> {
        if self.state != AlterShareGroupOffsetsState::Ready {
            return Err(AlterShareGroupOffsetsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AlterShareGroupOffsetsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = AlterShareGroupOffsetsState::AwaitingDriver;
        Ok(AlterShareGroupOffsetsTransition::one(
            AlterShareGroupOffsetsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AlterShareGroupOffsetsTransition, AlterShareGroupOffsetsMachineError> {
        if self.state != AlterShareGroupOffsetsState::AwaitingDriver {
            return Err(AlterShareGroupOffsetsMachineError::InvalidState);
        }
        self.state = AlterShareGroupOffsetsState::Submitted;
        Ok(AlterShareGroupOffsetsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: AlterShareGroupOffsetsBatch,
    ) -> Result<AlterShareGroupOffsetsTransition, AlterShareGroupOffsetsMachineError> {
        if self.state != AlterShareGroupOffsetsState::Submitted {
            return Err(AlterShareGroupOffsetsMachineError::InvalidState);
        }
        match correlate_response(&self.plan, batch) {
            ResponseValidation::Valid(batch) => {
                Ok(self.finish(AlterShareGroupOffsetsTerminal::Altered(batch)))
            }
            ResponseValidation::TooLarge => Ok(self.finish_failure(
                AlterShareGroupOffsetsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            )),
            ResponseValidation::Invalid => Ok(self.finish_failure(
                AlterShareGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            )),
        }
    }

    fn broker_rejected(
        &mut self,
        error: AlterShareGroupOffsetsBrokerError,
    ) -> Result<AlterShareGroupOffsetsTransition, AlterShareGroupOffsetsMachineError> {
        if self.state != AlterShareGroupOffsetsState::Submitted {
            return Err(AlterShareGroupOffsetsMachineError::InvalidState);
        }
        if !broker_error_is_valid(&error) {
            return Ok(self.finish_failure(
                AlterShareGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        Ok(self.finish(AlterShareGroupOffsetsTerminal::BrokerRejected(error)))
    }

    fn finish_awaiting(
        &mut self,
        kind: AlterShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterShareGroupOffsetsTransition, AlterShareGroupOffsetsMachineError> {
        if self.state != AlterShareGroupOffsetsState::AwaitingDriver {
            return Err(AlterShareGroupOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: AlterShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterShareGroupOffsetsTransition, AlterShareGroupOffsetsMachineError> {
        if self.state != AlterShareGroupOffsetsState::Submitted {
            return Err(AlterShareGroupOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: AlterShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> AlterShareGroupOffsetsTransition {
        self.finish(AlterShareGroupOffsetsTerminal::Failed(
            AlterShareGroupOffsetsFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: AlterShareGroupOffsetsTerminal,
    ) -> AlterShareGroupOffsetsTransition {
        self.state = AlterShareGroupOffsetsState::Completed;
        AlterShareGroupOffsetsTransition::one(AlterShareGroupOffsetsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
