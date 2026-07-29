//! Atomic legacy full-snapshot configuration transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    LegacyAlterConfigsBatch, LegacyAlterConfigsEffect, LegacyAlterConfigsFailure,
    LegacyAlterConfigsFailureKind, LegacyAlterConfigsInput, LegacyAlterConfigsMachine,
    LegacyAlterConfigsMachineError, LegacyAlterConfigsState, LegacyAlterConfigsTerminal,
    LegacyAlterConfigsTransition,
};

impl LegacyAlterConfigsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or fallback.
    pub fn apply(
        &mut self,
        input: LegacyAlterConfigsInput,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state == LegacyAlterConfigsState::Completed {
            return Err(LegacyAlterConfigsMachineError::AlreadyCompleted);
        }
        match input {
            LegacyAlterConfigsInput::Start { now } => self.start(now),
            LegacyAlterConfigsInput::DriverAccepted => self.driver_accepted(),
            LegacyAlterConfigsInput::DriverRejected => self.finish_awaiting(
                LegacyAlterConfigsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            LegacyAlterConfigsInput::DeadlineElapsed => self.finish_awaiting(
                LegacyAlterConfigsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            LegacyAlterConfigsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted_failure(LegacyAlterConfigsFailureKind::DeadlineElapsed, delivery),
            LegacyAlterConfigsInput::BrokerResponded { batch } => self.broker_responded(batch),
            LegacyAlterConfigsInput::ResponseTooLarge => self.finish_submitted_failure(
                LegacyAlterConfigsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            LegacyAlterConfigsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted_failure(LegacyAlterConfigsFailureKind::Compatibility, delivery),
            LegacyAlterConfigsInput::TransportFailed { delivery } => {
                self.finish_submitted_failure(LegacyAlterConfigsFailureKind::Transport, delivery)
            }
            LegacyAlterConfigsInput::InvalidResponse => self.finish_submitted_failure(
                LegacyAlterConfigsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state != LegacyAlterConfigsState::Ready {
            return Err(LegacyAlterConfigsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(LegacyAlterConfigsTerminal::Failed(
                LegacyAlterConfigsFailure::new(
                    LegacyAlterConfigsFailureKind::DeadlineElapsed,
                    DeliveryStatus::NotSent,
                ),
            )));
        }
        self.state = LegacyAlterConfigsState::AwaitingDriver;
        Ok(LegacyAlterConfigsTransition::one(
            LegacyAlterConfigsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state != LegacyAlterConfigsState::AwaitingDriver {
            return Err(LegacyAlterConfigsMachineError::InvalidState);
        }
        self.state = LegacyAlterConfigsState::Submitted;
        Ok(LegacyAlterConfigsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: LegacyAlterConfigsBatch,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state != LegacyAlterConfigsState::Submitted {
            return Err(LegacyAlterConfigsMachineError::InvalidState);
        }
        if !self.batch_is_correlated(&batch) {
            return Ok(self.finish(LegacyAlterConfigsTerminal::Failed(
                LegacyAlterConfigsFailure::new(
                    LegacyAlterConfigsFailureKind::InvalidResponse,
                    DeliveryStatus::PossiblySent,
                ),
            )));
        }
        Ok(self.finish(LegacyAlterConfigsTerminal::Configs(batch)))
    }

    fn finish_awaiting(
        &mut self,
        kind: LegacyAlterConfigsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state != LegacyAlterConfigsState::AwaitingDriver {
            return Err(LegacyAlterConfigsMachineError::InvalidState);
        }
        Ok(self.finish(LegacyAlterConfigsTerminal::Failed(
            LegacyAlterConfigsFailure::new(kind, delivery),
        )))
    }

    fn finish_submitted_failure(
        &mut self,
        kind: LegacyAlterConfigsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state != LegacyAlterConfigsState::Submitted {
            return Err(LegacyAlterConfigsMachineError::InvalidState);
        }
        Ok(self.finish(LegacyAlterConfigsTerminal::Failed(
            LegacyAlterConfigsFailure::new(kind, delivery),
        )))
    }

    fn batch_is_correlated(&self, batch: &LegacyAlterConfigsBatch) -> bool {
        if batch.resources().len() != self.plan.resources().len() {
            return false;
        }
        !self
            .plan
            .resources()
            .iter()
            .zip(batch.resources())
            .any(|(resource, outcome)| {
                resource.resource_type() != outcome.resource_type()
                    || resource.resource_name() != outcome.resource_name()
            })
    }

    fn finish(&mut self, terminal: LegacyAlterConfigsTerminal) -> LegacyAlterConfigsTransition {
        self.state = LegacyAlterConfigsState::Completed;
        LegacyAlterConfigsTransition::one(LegacyAlterConfigsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
