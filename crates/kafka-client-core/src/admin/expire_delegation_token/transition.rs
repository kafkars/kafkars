//! `AnyBroker` handoff and sole token-expiration terminal assignment.

use crate::DeliveryStatus;

use super::{
    ExpireDelegationTokenEffect, ExpireDelegationTokenFailure, ExpireDelegationTokenFailureKind,
    ExpireDelegationTokenInput, ExpireDelegationTokenMachine, ExpireDelegationTokenMachineError,
    ExpireDelegationTokenState, ExpireDelegationTokenSuccess, ExpireDelegationTokenTerminal,
    ExpireDelegationTokenTransition,
};

impl ExpireDelegationTokenMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "the transition boundary consumes one normalized input capability"
    )]
    pub fn apply(
        &mut self,
        input: ExpireDelegationTokenInput,
    ) -> Result<ExpireDelegationTokenTransition, ExpireDelegationTokenMachineError> {
        if self.state == ExpireDelegationTokenState::Completed {
            return Err(ExpireDelegationTokenMachineError::AlreadyCompleted);
        }
        match input {
            ExpireDelegationTokenInput::Start { now } => self.start(now),
            ExpireDelegationTokenInput::DriverAccepted => self.driver_accepted(),
            ExpireDelegationTokenInput::DriverRejected => self.finish_awaiting(
                ExpireDelegationTokenFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            ExpireDelegationTokenInput::DeadlineElapsed => self.finish_awaiting(
                ExpireDelegationTokenFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            ExpireDelegationTokenInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(ExpireDelegationTokenFailureKind::DeadlineElapsed, delivery)
            }
            ExpireDelegationTokenInput::BrokerResponded { response } => {
                let (throttle_time_ms, expiry_timestamp_ms) = response.into_parts();
                self.finish_submitted_terminal(ExpireDelegationTokenTerminal::Expired(
                    ExpireDelegationTokenSuccess::new(throttle_time_ms, expiry_timestamp_ms),
                ))
            }
            ExpireDelegationTokenInput::BrokerRejected { error } => {
                self.finish_submitted_terminal(ExpireDelegationTokenTerminal::BrokerRejected(error))
            }
            ExpireDelegationTokenInput::ResponseTooLarge => self.finish_submitted(
                ExpireDelegationTokenFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            ExpireDelegationTokenInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(ExpireDelegationTokenFailureKind::Compatibility, delivery)
            }
            ExpireDelegationTokenInput::TransportFailed { delivery } => {
                self.finish_submitted(ExpireDelegationTokenFailureKind::Transport, delivery)
            }
            ExpireDelegationTokenInput::InvalidResponse => self.finish_submitted(
                ExpireDelegationTokenFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<ExpireDelegationTokenTransition, ExpireDelegationTokenMachineError> {
        if self.state != ExpireDelegationTokenState::Ready {
            return Err(ExpireDelegationTokenMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                ExpireDelegationTokenFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        let plan = self
            .plan
            .take()
            .ok_or(ExpireDelegationTokenMachineError::InvalidState)?;
        self.state = ExpireDelegationTokenState::AwaitingDriver;
        Ok(ExpireDelegationTokenTransition::one(
            ExpireDelegationTokenEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<ExpireDelegationTokenTransition, ExpireDelegationTokenMachineError> {
        if self.state != ExpireDelegationTokenState::AwaitingDriver {
            return Err(ExpireDelegationTokenMachineError::InvalidState);
        }
        self.state = ExpireDelegationTokenState::Submitted;
        Ok(ExpireDelegationTokenTransition::none())
    }

    fn finish_awaiting(
        &mut self,
        kind: ExpireDelegationTokenFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ExpireDelegationTokenTransition, ExpireDelegationTokenMachineError> {
        if self.state != ExpireDelegationTokenState::AwaitingDriver {
            return Err(ExpireDelegationTokenMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: ExpireDelegationTokenFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ExpireDelegationTokenTransition, ExpireDelegationTokenMachineError> {
        if self.state != ExpireDelegationTokenState::Submitted {
            return Err(ExpireDelegationTokenMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: ExpireDelegationTokenTerminal,
    ) -> Result<ExpireDelegationTokenTransition, ExpireDelegationTokenMachineError> {
        if self.state != ExpireDelegationTokenState::Submitted {
            return Err(ExpireDelegationTokenMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: ExpireDelegationTokenFailureKind,
        delivery: DeliveryStatus,
    ) -> ExpireDelegationTokenTransition {
        self.finish(ExpireDelegationTokenTerminal::Failed(
            ExpireDelegationTokenFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: ExpireDelegationTokenTerminal,
    ) -> ExpireDelegationTokenTransition {
        self.state = ExpireDelegationTokenState::Completed;
        ExpireDelegationTokenTransition::one(ExpireDelegationTokenEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
