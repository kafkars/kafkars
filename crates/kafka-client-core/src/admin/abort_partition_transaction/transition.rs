//! Destructive partition-transaction abort transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    AbortPartitionTransactionEffect, AbortPartitionTransactionFailure,
    AbortPartitionTransactionFailureKind, AbortPartitionTransactionInput,
    AbortPartitionTransactionMachine, AbortPartitionTransactionMachineError,
    AbortPartitionTransactionState, AbortPartitionTransactionTerminal,
    AbortPartitionTransactionTransition,
};

impl AbortPartitionTransactionMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AbortPartitionTransactionInput,
    ) -> Result<AbortPartitionTransactionTransition, AbortPartitionTransactionMachineError> {
        if self.state == AbortPartitionTransactionState::Completed {
            return Err(AbortPartitionTransactionMachineError::AlreadyCompleted);
        }
        match input {
            AbortPartitionTransactionInput::Start { now } => self.start(now),
            AbortPartitionTransactionInput::DriverAccepted => self.driver_accepted(),
            AbortPartitionTransactionInput::DriverRejected => self.finish_awaiting(
                AbortPartitionTransactionFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            AbortPartitionTransactionInput::DeadlineElapsed => self.finish_awaiting(
                AbortPartitionTransactionFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            AbortPartitionTransactionInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    AbortPartitionTransactionFailureKind::DeadlineElapsed,
                    delivery,
                ),
            AbortPartitionTransactionInput::BrokerResponded => {
                self.finish_submitted_terminal(AbortPartitionTransactionTerminal::Aborted)
            }
            AbortPartitionTransactionInput::BrokerRejected { error } => self
                .finish_submitted_terminal(AbortPartitionTransactionTerminal::BrokerRejected(
                    error,
                )),
            AbortPartitionTransactionInput::ResponseTooLarge => self.finish_submitted(
                AbortPartitionTransactionFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AbortPartitionTransactionInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    AbortPartitionTransactionFailureKind::Compatibility,
                    delivery,
                ),
            AbortPartitionTransactionInput::TransportFailed { delivery } => {
                self.finish_submitted(AbortPartitionTransactionFailureKind::Transport, delivery)
            }
            AbortPartitionTransactionInput::InvalidResponse => self.finish_submitted(
                AbortPartitionTransactionFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AbortPartitionTransactionTransition, AbortPartitionTransactionMachineError> {
        if self.state != AbortPartitionTransactionState::Ready {
            return Err(AbortPartitionTransactionMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AbortPartitionTransactionFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = AbortPartitionTransactionState::AwaitingDriver;
        Ok(AbortPartitionTransactionTransition::one(
            AbortPartitionTransactionEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AbortPartitionTransactionTransition, AbortPartitionTransactionMachineError> {
        if self.state != AbortPartitionTransactionState::AwaitingDriver {
            return Err(AbortPartitionTransactionMachineError::InvalidState);
        }
        self.state = AbortPartitionTransactionState::Submitted;
        Ok(AbortPartitionTransactionTransition::none())
    }

    fn finish_awaiting(
        &mut self,
        kind: AbortPartitionTransactionFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AbortPartitionTransactionTransition, AbortPartitionTransactionMachineError> {
        if self.state != AbortPartitionTransactionState::AwaitingDriver {
            return Err(AbortPartitionTransactionMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: AbortPartitionTransactionFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AbortPartitionTransactionTransition, AbortPartitionTransactionMachineError> {
        if self.state != AbortPartitionTransactionState::Submitted {
            return Err(AbortPartitionTransactionMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: AbortPartitionTransactionTerminal,
    ) -> Result<AbortPartitionTransactionTransition, AbortPartitionTransactionMachineError> {
        if self.state != AbortPartitionTransactionState::Submitted {
            return Err(AbortPartitionTransactionMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: AbortPartitionTransactionFailureKind,
        delivery: DeliveryStatus,
    ) -> AbortPartitionTransactionTransition {
        self.finish(AbortPartitionTransactionTerminal::Failed(
            AbortPartitionTransactionFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: AbortPartitionTransactionTerminal,
    ) -> AbortPartitionTransactionTransition {
        self.state = AbortPartitionTransactionState::Completed;
        AbortPartitionTransactionTransition::one(AbortPartitionTransactionEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
