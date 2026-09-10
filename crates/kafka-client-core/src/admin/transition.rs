//! Atomic `CreateTopics` lifecycle transitions and terminal single assignment.

use crate::DeliveryStatus;

use super::{
    CreateTopicOutcome, CreateTopicResult, CreateTopicsEffect, CreateTopicsFailure,
    CreateTopicsInput, CreateTopicsMachine, CreateTopicsMachineError, CreateTopicsState,
    CreateTopicsTerminal, CreateTopicsTransition,
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
            CreateTopicsInput::DeadlineElapsed => self.deadline_elapsed(),
            CreateTopicsInput::BrokerResponded { outcomes } => self.broker_responded(outcomes),
            CreateTopicsInput::VisibilityConfirmed => self.visibility_confirmed(),
            CreateTopicsInput::VisibilityFailed => self.visibility_failed(),
            CreateTopicsInput::TransportFailed { delivery } => self.transport_failed(delivery),
            CreateTopicsInput::InvalidResponse => self.invalid_response(),
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

    fn deadline_elapsed(&mut self) -> Result<CreateTopicsTransition, CreateTopicsMachineError> {
        if self.state != CreateTopicsState::AwaitingDriver {
            return Err(CreateTopicsMachineError::InvalidState);
        }
        Ok(self.finish(CreateTopicsTerminal::Failed(
            CreateTopicsFailure::deadline_elapsed(),
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
        let requires_visibility = !self.plan.validate_only()
            && outcomes
                .iter()
                .any(|outcome| matches!(outcome.result(), CreateTopicResult::Created));
        if !requires_visibility {
            return Ok(self.finish(CreateTopicsTerminal::Topics(outcomes)));
        }
        self.pending_outcomes = Some(outcomes);
        self.state = CreateTopicsState::AwaitingVisibility;
        Ok(CreateTopicsTransition::one(
            CreateTopicsEffect::ConfirmVisibility {
                operation_id: self.operation_id,
                deadline: self.deadline,
            },
        ))
    }

    fn visibility_confirmed(&mut self) -> Result<CreateTopicsTransition, CreateTopicsMachineError> {
        if self.state != CreateTopicsState::AwaitingVisibility {
            return Err(CreateTopicsMachineError::InvalidState);
        }
        let outcomes = self
            .pending_outcomes
            .take()
            .ok_or(CreateTopicsMachineError::MissingBrokerOutcomes)?;
        Ok(self.finish(CreateTopicsTerminal::Topics(outcomes)))
    }

    fn visibility_failed(&mut self) -> Result<CreateTopicsTransition, CreateTopicsMachineError> {
        if self.state != CreateTopicsState::AwaitingVisibility {
            return Err(CreateTopicsMachineError::InvalidState);
        }
        self.pending_outcomes = None;
        Ok(self.finish(CreateTopicsTerminal::Failed(
            CreateTopicsFailure::visibility_unconfirmed(),
        )))
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

    fn invalid_response(&mut self) -> Result<CreateTopicsTransition, CreateTopicsMachineError> {
        if self.state != CreateTopicsState::Submitted {
            return Err(CreateTopicsMachineError::InvalidState);
        }
        Ok(self.finish(CreateTopicsTerminal::Failed(
            CreateTopicsFailure::invalid_response(),
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
