//! Atomic incremental configuration transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    IncrementalAlterConfigsBatch, IncrementalAlterConfigsEffect, IncrementalAlterConfigsFailure,
    IncrementalAlterConfigsFailureKind, IncrementalAlterConfigsInput,
    IncrementalAlterConfigsMachine, IncrementalAlterConfigsMachineError,
    IncrementalAlterConfigsState, IncrementalAlterConfigsTerminal,
    IncrementalAlterConfigsTransition,
};

impl IncrementalAlterConfigsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or fallback.
    pub fn apply(
        &mut self,
        input: IncrementalAlterConfigsInput,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state == IncrementalAlterConfigsState::Completed {
            return Err(IncrementalAlterConfigsMachineError::AlreadyCompleted);
        }
        match input {
            IncrementalAlterConfigsInput::Start { now } => self.start(now),
            IncrementalAlterConfigsInput::DriverAccepted => self.driver_accepted(),
            IncrementalAlterConfigsInput::DriverRejected => self.finish_awaiting(
                IncrementalAlterConfigsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            IncrementalAlterConfigsInput::DeadlineElapsed => self.finish_awaiting(
                IncrementalAlterConfigsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            IncrementalAlterConfigsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted_failure(
                    IncrementalAlterConfigsFailureKind::DeadlineElapsed,
                    delivery,
                ),
            IncrementalAlterConfigsInput::BrokerResponded { batch } => self.broker_responded(batch),
            IncrementalAlterConfigsInput::ResponseTooLarge => self.finish_submitted_failure(
                IncrementalAlterConfigsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            IncrementalAlterConfigsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted_failure(
                    IncrementalAlterConfigsFailureKind::Compatibility,
                    delivery,
                ),
            IncrementalAlterConfigsInput::TransportFailed { delivery } => self
                .finish_submitted_failure(IncrementalAlterConfigsFailureKind::Transport, delivery),
            IncrementalAlterConfigsInput::InvalidResponse => self.finish_submitted_failure(
                IncrementalAlterConfigsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state != IncrementalAlterConfigsState::Ready {
            return Err(IncrementalAlterConfigsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(IncrementalAlterConfigsTerminal::Failed(
                IncrementalAlterConfigsFailure::new(
                    IncrementalAlterConfigsFailureKind::DeadlineElapsed,
                    DeliveryStatus::NotSent,
                ),
            )));
        }
        self.state = IncrementalAlterConfigsState::AwaitingDriver;
        Ok(IncrementalAlterConfigsTransition::one(
            IncrementalAlterConfigsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state != IncrementalAlterConfigsState::AwaitingDriver {
            return Err(IncrementalAlterConfigsMachineError::InvalidState);
        }
        self.state = IncrementalAlterConfigsState::Submitted;
        Ok(IncrementalAlterConfigsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: IncrementalAlterConfigsBatch,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state != IncrementalAlterConfigsState::Submitted {
            return Err(IncrementalAlterConfigsMachineError::InvalidState);
        }
        if !self.batch_is_correlated(&batch) {
            return Ok(self.finish(IncrementalAlterConfigsTerminal::Failed(
                IncrementalAlterConfigsFailure::new(
                    IncrementalAlterConfigsFailureKind::InvalidResponse,
                    DeliveryStatus::PossiblySent,
                ),
            )));
        }
        Ok(self.finish(IncrementalAlterConfigsTerminal::Configs(batch)))
    }

    fn finish_awaiting(
        &mut self,
        kind: IncrementalAlterConfigsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state != IncrementalAlterConfigsState::AwaitingDriver {
            return Err(IncrementalAlterConfigsMachineError::InvalidState);
        }
        Ok(self.finish(IncrementalAlterConfigsTerminal::Failed(
            IncrementalAlterConfigsFailure::new(kind, delivery),
        )))
    }

    fn finish_submitted_failure(
        &mut self,
        kind: IncrementalAlterConfigsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state != IncrementalAlterConfigsState::Submitted {
            return Err(IncrementalAlterConfigsMachineError::InvalidState);
        }
        Ok(self.finish(IncrementalAlterConfigsTerminal::Failed(
            IncrementalAlterConfigsFailure::new(kind, delivery),
        )))
    }

    fn batch_is_correlated(&self, batch: &IncrementalAlterConfigsBatch) -> bool {
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

    fn finish(
        &mut self,
        terminal: IncrementalAlterConfigsTerminal,
    ) -> IncrementalAlterConfigsTransition {
        self.state = IncrementalAlterConfigsState::Completed;
        IncrementalAlterConfigsTransition::one(IncrementalAlterConfigsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
