//! Atomic `CreateTopics` lifecycle transitions and terminal single assignment.

use crate::DeliveryStatus;

use super::{
    CreateTopicOutcome, CreateTopicsEffect, CreateTopicsFailure, CreateTopicsInput,
    CreateTopicsMachine, CreateTopicsMachineError, CreateTopicsState, CreateTopicsTerminal,
    CreateTopicsTransition,
};

impl CreateTopicsMachine {
    /// Applies one normalized fact without hidden I/O or retry behavior.
    pub fn apply(
        &mut self,
        input: CreateTopicsInput,
    ) -> Result<CreateTopicsTransition, CreateTopicsMachineError> {
        if self.state == CreateTopicsState::Completed {
            return Err(CreateTopicsMachineError::AlreadyCompleted);
        }
        match input {
            CreateTopicsInput::Start { now } => self.start(now),
            CreateTopicsInput::DriverAccepted => self.driver_accepted(),
            CreateTopicsInput::DriverRejected => self.driver_rejected(),
            CreateTopicsInput::BrokerResponded { outcomes } => self.broker_responded(outcomes),
            CreateTopicsInput::TransportFailed { delivery } => self.transport_failed(delivery),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<CreateTopicsTransition, CreateTopicsMachineError> {
        if self.state != CreateTopicsState::Ready {
            return Err(CreateTopicsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(CreateTopicsTerminal::Failed(
                CreateTopicsFailure::deadline_elapsed(),
            )));
        }
        self.state = CreateTopicsState::AwaitingDriver;
        Ok(CreateTopicsTransition::one(CreateTopicsEffect::Submit {
            operation_id: self.operation_id,
            deadline: self.deadline,
            plan: self.plan.clone(),
        }))
    }

    fn driver_accepted(&mut self) -> Result<CreateTopicsTransition, CreateTopicsMachineError> {
        if self.state != CreateTopicsState::AwaitingDriver {
            return Err(CreateTopicsMachineError::InvalidState);
        }
        self.state = CreateTopicsState::Submitted;
        Ok(CreateTopicsTransition::none())
    }

    fn driver_rejected(&mut self) -> Result<CreateTopicsTransition, CreateTopicsMachineError> {
        if self.state != CreateTopicsState::AwaitingDriver {
            return Err(CreateTopicsMachineError::InvalidState);
        }
        Ok(self.finish(CreateTopicsTerminal::Failed(
            CreateTopicsFailure::driver_rejected(),
        )))
    }

    fn broker_responded(
        &mut self,
        outcomes: Vec<CreateTopicOutcome>,
    ) -> Result<CreateTopicsTransition, CreateTopicsMachineError> {
        if self.state != CreateTopicsState::Submitted {
            return Err(CreateTopicsMachineError::InvalidState);
        }
        self.validate_outcomes(&outcomes)?;
        Ok(self.finish(CreateTopicsTerminal::Topics(outcomes)))
    }

    fn transport_failed(
        &mut self,
        delivery: DeliveryStatus,
    ) -> Result<CreateTopicsTransition, CreateTopicsMachineError> {
        if self.state != CreateTopicsState::Submitted {
            return Err(CreateTopicsMachineError::InvalidState);
        }
        Ok(self.finish(CreateTopicsTerminal::Failed(
            CreateTopicsFailure::transport(delivery),
        )))
    }

    fn validate_outcomes(
        &self,
        outcomes: &[CreateTopicOutcome],
    ) -> Result<(), CreateTopicsMachineError> {
        if self.plan.topics().len() != outcomes.len() {
            return Err(CreateTopicsMachineError::OutcomeCountMismatch);
        }
        if self
            .plan
            .topics()
            .iter()
            .zip(outcomes)
            .any(|(topic, outcome)| topic.name() != outcome.topic())
        {
            return Err(CreateTopicsMachineError::OutcomeTopicMismatch);
        }
        Ok(())
    }

    fn finish(&mut self, terminal: CreateTopicsTerminal) -> CreateTopicsTransition {
        self.state = CreateTopicsState::Completed;
        CreateTopicsTransition::one(CreateTopicsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
