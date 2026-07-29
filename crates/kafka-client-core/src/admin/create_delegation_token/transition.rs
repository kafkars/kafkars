//! AnyBroker handoff, response correlation, and sole terminal assignment.

use crate::DeliveryStatus;

use super::{
    CreateDelegationTokenEffect, CreateDelegationTokenFailure, CreateDelegationTokenFailureKind,
    CreateDelegationTokenInput, CreateDelegationTokenMachine, CreateDelegationTokenMachineError,
    CreateDelegationTokenState, CreateDelegationTokenSuccess, CreateDelegationTokenTerminal,
    CreateDelegationTokenTransition, DelegationToken,
};

impl CreateDelegationTokenMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: CreateDelegationTokenInput,
    ) -> Result<CreateDelegationTokenTransition, CreateDelegationTokenMachineError> {
        if self.state == CreateDelegationTokenState::Completed {
            return Err(CreateDelegationTokenMachineError::AlreadyCompleted);
        }
        match input {
            CreateDelegationTokenInput::Start { now } => self.start(now),
            CreateDelegationTokenInput::DriverAccepted => self.driver_accepted(),
            CreateDelegationTokenInput::DriverRejected => self.finish_awaiting(
                CreateDelegationTokenFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            CreateDelegationTokenInput::DeadlineElapsed => self.finish_awaiting(
                CreateDelegationTokenFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            CreateDelegationTokenInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(CreateDelegationTokenFailureKind::DeadlineElapsed, delivery)
            }
            CreateDelegationTokenInput::BrokerResponded { response } => {
                self.broker_responded(response)
            }
            CreateDelegationTokenInput::BrokerRejected { error } => {
                self.finish_submitted_terminal(CreateDelegationTokenTerminal::BrokerRejected(error))
            }
            CreateDelegationTokenInput::ResponseTooLarge => self.finish_submitted(
                CreateDelegationTokenFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            CreateDelegationTokenInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(CreateDelegationTokenFailureKind::Compatibility, delivery)
            }
            CreateDelegationTokenInput::TransportFailed { delivery } => {
                self.finish_submitted(CreateDelegationTokenFailureKind::Transport, delivery)
            }
            CreateDelegationTokenInput::InvalidResponse => self.finish_submitted(
                CreateDelegationTokenFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<CreateDelegationTokenTransition, CreateDelegationTokenMachineError> {
        if self.state != CreateDelegationTokenState::Ready {
            return Err(CreateDelegationTokenMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                CreateDelegationTokenFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = CreateDelegationTokenState::AwaitingDriver;
        Ok(CreateDelegationTokenTransition::one(
            CreateDelegationTokenEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<CreateDelegationTokenTransition, CreateDelegationTokenMachineError> {
        if self.state != CreateDelegationTokenState::AwaitingDriver {
            return Err(CreateDelegationTokenMachineError::InvalidState);
        }
        self.state = CreateDelegationTokenState::Submitted;
        Ok(CreateDelegationTokenTransition::none())
    }

    fn broker_responded(
        &mut self,
        response: super::CreateDelegationTokenResponse,
    ) -> Result<CreateDelegationTokenTransition, CreateDelegationTokenMachineError> {
        if self.state != CreateDelegationTokenState::Submitted {
            return Err(CreateDelegationTokenMachineError::InvalidState);
        }
        if self
            .plan
            .owner()
            .is_some_and(|owner| owner != response.owner())
        {
            return Ok(self.finish_failure(
                CreateDelegationTokenFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        let (
            throttle_time_ms,
            owner,
            requester,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
        ) = response.into_parts();
        let token = DelegationToken::new(
            owner,
            requester,
            self.plan.renewers().to_vec(),
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
        );
        Ok(self.finish(CreateDelegationTokenTerminal::Created(
            CreateDelegationTokenSuccess::new(throttle_time_ms, token),
        )))
    }

    fn finish_awaiting(
        &mut self,
        kind: CreateDelegationTokenFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<CreateDelegationTokenTransition, CreateDelegationTokenMachineError> {
        if self.state != CreateDelegationTokenState::AwaitingDriver {
            return Err(CreateDelegationTokenMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: CreateDelegationTokenFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<CreateDelegationTokenTransition, CreateDelegationTokenMachineError> {
        if self.state != CreateDelegationTokenState::Submitted {
            return Err(CreateDelegationTokenMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: CreateDelegationTokenTerminal,
    ) -> Result<CreateDelegationTokenTransition, CreateDelegationTokenMachineError> {
        if self.state != CreateDelegationTokenState::Submitted {
            return Err(CreateDelegationTokenMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: CreateDelegationTokenFailureKind,
        delivery: DeliveryStatus,
    ) -> CreateDelegationTokenTransition {
        self.finish(CreateDelegationTokenTerminal::Failed(
            CreateDelegationTokenFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: CreateDelegationTokenTerminal,
    ) -> CreateDelegationTokenTransition {
        self.state = CreateDelegationTokenState::Completed;
        CreateDelegationTokenTransition::one(CreateDelegationTokenEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
