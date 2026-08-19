//! `AnyBroker` handoff and sole token-renewal terminal assignment.

use crate::DeliveryStatus;

use super::{
    RenewDelegationTokenEffect, RenewDelegationTokenFailure, RenewDelegationTokenFailureKind,
    RenewDelegationTokenInput, RenewDelegationTokenMachine, RenewDelegationTokenMachineError,
    RenewDelegationTokenState, RenewDelegationTokenSuccess, RenewDelegationTokenTerminal,
    RenewDelegationTokenTransition,
};

impl RenewDelegationTokenMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "the transition boundary consumes one normalized input capability"
    )]
    pub fn apply(
        &mut self,
        input: RenewDelegationTokenInput,
    ) -> Result<RenewDelegationTokenTransition, RenewDelegationTokenMachineError> {
        if self.state == RenewDelegationTokenState::Completed {
            return Err(RenewDelegationTokenMachineError::AlreadyCompleted);
        }
        match input {
            RenewDelegationTokenInput::Start { now } => self.start(now),
            RenewDelegationTokenInput::DriverAccepted => self.driver_accepted(),
            RenewDelegationTokenInput::DriverRejected => self.finish_awaiting(
                RenewDelegationTokenFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            RenewDelegationTokenInput::DeadlineElapsed => self.finish_awaiting(
                RenewDelegationTokenFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            RenewDelegationTokenInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(RenewDelegationTokenFailureKind::DeadlineElapsed, delivery)
            }
            RenewDelegationTokenInput::BrokerResponded { response } => {
                let (throttle_time_ms, expiry_timestamp_ms) = response.into_parts();
                self.finish_submitted_terminal(RenewDelegationTokenTerminal::Renewed(
                    RenewDelegationTokenSuccess::new(throttle_time_ms, expiry_timestamp_ms),
                ))
            }
            RenewDelegationTokenInput::BrokerRejected { error } => {
                self.finish_submitted_terminal(RenewDelegationTokenTerminal::BrokerRejected(error))
            }
            RenewDelegationTokenInput::ResponseTooLarge => self.finish_submitted(
                RenewDelegationTokenFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            RenewDelegationTokenInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(RenewDelegationTokenFailureKind::Compatibility, delivery)
            }
            RenewDelegationTokenInput::TransportFailed { delivery } => {
                self.finish_submitted(RenewDelegationTokenFailureKind::Transport, delivery)
            }
            RenewDelegationTokenInput::InvalidResponse => self.finish_submitted(
                RenewDelegationTokenFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<RenewDelegationTokenTransition, RenewDelegationTokenMachineError> {
        if self.state != RenewDelegationTokenState::Ready {
            return Err(RenewDelegationTokenMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                RenewDelegationTokenFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        let plan = self
            .plan
            .take()
            .ok_or(RenewDelegationTokenMachineError::InvalidState)?;
        self.state = RenewDelegationTokenState::AwaitingDriver;
        Ok(RenewDelegationTokenTransition::one(
            RenewDelegationTokenEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<RenewDelegationTokenTransition, RenewDelegationTokenMachineError> {
        if self.state != RenewDelegationTokenState::AwaitingDriver {
            return Err(RenewDelegationTokenMachineError::InvalidState);
        }
        self.state = RenewDelegationTokenState::Submitted;
        Ok(RenewDelegationTokenTransition::none())
    }

    fn finish_awaiting(
        &mut self,
        kind: RenewDelegationTokenFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<RenewDelegationTokenTransition, RenewDelegationTokenMachineError> {
        if self.state != RenewDelegationTokenState::AwaitingDriver {
            return Err(RenewDelegationTokenMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: RenewDelegationTokenFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<RenewDelegationTokenTransition, RenewDelegationTokenMachineError> {
        if self.state != RenewDelegationTokenState::Submitted {
            return Err(RenewDelegationTokenMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: RenewDelegationTokenTerminal,
    ) -> Result<RenewDelegationTokenTransition, RenewDelegationTokenMachineError> {
        if self.state != RenewDelegationTokenState::Submitted {
            return Err(RenewDelegationTokenMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: RenewDelegationTokenFailureKind,
        delivery: DeliveryStatus,
    ) -> RenewDelegationTokenTransition {
        self.finish(RenewDelegationTokenTerminal::Failed(
            RenewDelegationTokenFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: RenewDelegationTokenTerminal) -> RenewDelegationTokenTransition {
        self.state = RenewDelegationTokenState::Completed;
        RenewDelegationTokenTransition::one(RenewDelegationTokenEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
