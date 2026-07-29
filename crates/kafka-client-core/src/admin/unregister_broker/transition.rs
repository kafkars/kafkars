//! Destructive broker-unregistration transitions and sole terminal assignment.

use crate::DeliveryStatus;

use super::{
    UNREGISTER_BROKER_DIAGNOSTIC_BYTES, UnregisterBrokerEffect, UnregisterBrokerFailure,
    UnregisterBrokerFailureKind, UnregisterBrokerInput, UnregisterBrokerMachine,
    UnregisterBrokerMachineError, UnregisterBrokerState, UnregisterBrokerTerminal,
    UnregisterBrokerTransition,
};

impl UnregisterBrokerMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: UnregisterBrokerInput,
    ) -> Result<UnregisterBrokerTransition, UnregisterBrokerMachineError> {
        if self.state == UnregisterBrokerState::Completed {
            return Err(UnregisterBrokerMachineError::AlreadyCompleted);
        }
        match input {
            UnregisterBrokerInput::Start { now } => self.start(now),
            UnregisterBrokerInput::DriverAccepted => self.driver_accepted(),
            UnregisterBrokerInput::DriverRejected => self.finish_awaiting(
                UnregisterBrokerFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            UnregisterBrokerInput::DeadlineElapsed => self.finish_awaiting(
                UnregisterBrokerFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            UnregisterBrokerInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(UnregisterBrokerFailureKind::DeadlineElapsed, delivery)
            }
            UnregisterBrokerInput::BrokerResponded { success } => {
                self.finish_submitted_terminal(UnregisterBrokerTerminal::Unregistered(success))
            }
            UnregisterBrokerInput::BrokerRejected { error } => {
                if diagnostic_is_invalid(&error) {
                    self.finish_submitted(
                        UnregisterBrokerFailureKind::InvalidResponse,
                        DeliveryStatus::PossiblySent,
                    )
                } else {
                    self.finish_submitted_terminal(UnregisterBrokerTerminal::BrokerRejected(error))
                }
            }
            UnregisterBrokerInput::ResponseTooLarge => self.finish_submitted(
                UnregisterBrokerFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            UnregisterBrokerInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(UnregisterBrokerFailureKind::Compatibility, delivery)
            }
            UnregisterBrokerInput::TransportFailed { delivery } => {
                self.finish_submitted(UnregisterBrokerFailureKind::Transport, delivery)
            }
            UnregisterBrokerInput::InvalidResponse => self.finish_submitted(
                UnregisterBrokerFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<UnregisterBrokerTransition, UnregisterBrokerMachineError> {
        if self.state != UnregisterBrokerState::Ready {
            return Err(UnregisterBrokerMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                UnregisterBrokerFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = UnregisterBrokerState::AwaitingDriver;
        Ok(UnregisterBrokerTransition::one(
            UnregisterBrokerEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<UnregisterBrokerTransition, UnregisterBrokerMachineError> {
        if self.state != UnregisterBrokerState::AwaitingDriver {
            return Err(UnregisterBrokerMachineError::InvalidState);
        }
        self.state = UnregisterBrokerState::Submitted;
        Ok(UnregisterBrokerTransition::none())
    }

    fn finish_awaiting(
        &mut self,
        kind: UnregisterBrokerFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<UnregisterBrokerTransition, UnregisterBrokerMachineError> {
        if self.state != UnregisterBrokerState::AwaitingDriver {
            return Err(UnregisterBrokerMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: UnregisterBrokerFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<UnregisterBrokerTransition, UnregisterBrokerMachineError> {
        if self.state != UnregisterBrokerState::Submitted {
            return Err(UnregisterBrokerMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: UnregisterBrokerTerminal,
    ) -> Result<UnregisterBrokerTransition, UnregisterBrokerMachineError> {
        if self.state != UnregisterBrokerState::Submitted {
            return Err(UnregisterBrokerMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: UnregisterBrokerFailureKind,
        delivery: DeliveryStatus,
    ) -> UnregisterBrokerTransition {
        self.finish(UnregisterBrokerTerminal::Failed(
            UnregisterBrokerFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: UnregisterBrokerTerminal) -> UnregisterBrokerTransition {
        self.state = UnregisterBrokerState::Completed;
        UnregisterBrokerTransition::one(UnregisterBrokerEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn diagnostic_is_invalid(error: &super::UnregisterBrokerBrokerError) -> bool {
    error
        .message()
        .is_some_and(|message| message.len() > UNREGISTER_BROKER_DIAGNOSTIC_BYTES)
        || (error.message().is_none() && error.message_truncated())
}
