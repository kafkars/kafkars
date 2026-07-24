//! Atomic `DeleteTopics` lifecycle transitions and terminal single assignment.

use crate::DeliveryStatus;

use super::{
    DeleteTopicOutcome, DeleteTopicsEffect, DeleteTopicsFailure, DeleteTopicsInput,
    DeleteTopicsMachine, DeleteTopicsMachineError, DeleteTopicsState, DeleteTopicsTerminal,
    DeleteTopicsTransition,
};

impl DeleteTopicsMachine {
    /// Applies one normalized fact without hidden I/O or retry behavior.
    pub fn apply(
        &mut self,
        input: DeleteTopicsInput,
    ) -> Result<DeleteTopicsTransition, DeleteTopicsMachineError> {
        if self.state == DeleteTopicsState::Completed {
            return Err(DeleteTopicsMachineError::AlreadyCompleted);
        }
        match input {
            DeleteTopicsInput::Start { now } => self.start(now),
            DeleteTopicsInput::DriverAccepted => self.driver_accepted(),
            DeleteTopicsInput::DriverRejected => self.driver_rejected(),
            DeleteTopicsInput::DeadlineElapsed => self.deadline_elapsed(),
            DeleteTopicsInput::BrokerResponded { outcomes } => self.broker_responded(outcomes),
            DeleteTopicsInput::TransportFailed { delivery } => self.transport_failed(delivery),
            DeleteTopicsInput::InvalidResponse => self.invalid_response(),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DeleteTopicsTransition, DeleteTopicsMachineError> {
        if self.state != DeleteTopicsState::Ready {
            return Err(DeleteTopicsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(DeleteTopicsTerminal::Failed(
                DeleteTopicsFailure::deadline_elapsed(),
            )));
        }
        self.state = DeleteTopicsState::AwaitingDriver;
        Ok(DeleteTopicsTransition::one(DeleteTopicsEffect::Submit {
            operation_id: self.operation_id,
            deadline: self.deadline,
            plan: self.plan.clone(),
        }))
    }

    fn driver_accepted(&mut self) -> Result<DeleteTopicsTransition, DeleteTopicsMachineError> {
        if self.state != DeleteTopicsState::AwaitingDriver {
            return Err(DeleteTopicsMachineError::InvalidState);
        }
        self.state = DeleteTopicsState::Submitted;
        Ok(DeleteTopicsTransition::none())
    }

    fn driver_rejected(&mut self) -> Result<DeleteTopicsTransition, DeleteTopicsMachineError> {
        if self.state != DeleteTopicsState::AwaitingDriver {
            return Err(DeleteTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DeleteTopicsTerminal::Failed(
            DeleteTopicsFailure::driver_rejected(),
        )))
    }

    fn deadline_elapsed(&mut self) -> Result<DeleteTopicsTransition, DeleteTopicsMachineError> {
        if self.state != DeleteTopicsState::AwaitingDriver {
            return Err(DeleteTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DeleteTopicsTerminal::Failed(
            DeleteTopicsFailure::deadline_elapsed(),
        )))
    }

    fn broker_responded(
        &mut self,
        outcomes: Vec<DeleteTopicOutcome>,
    ) -> Result<DeleteTopicsTransition, DeleteTopicsMachineError> {
        if self.state != DeleteTopicsState::Submitted {
            return Err(DeleteTopicsMachineError::InvalidState);
        }
        self.validate_outcomes(&outcomes)?;
        Ok(self.finish(DeleteTopicsTerminal::Topics(outcomes)))
    }

    fn transport_failed(
        &mut self,
        delivery: DeliveryStatus,
    ) -> Result<DeleteTopicsTransition, DeleteTopicsMachineError> {
        if self.state != DeleteTopicsState::Submitted {
            return Err(DeleteTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DeleteTopicsTerminal::Failed(
            DeleteTopicsFailure::transport(delivery),
        )))
    }

    fn invalid_response(&mut self) -> Result<DeleteTopicsTransition, DeleteTopicsMachineError> {
        if self.state != DeleteTopicsState::Submitted {
            return Err(DeleteTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DeleteTopicsTerminal::Failed(
            DeleteTopicsFailure::invalid_response(),
        )))
    }

    fn validate_outcomes(
        &self,
        outcomes: &[DeleteTopicOutcome],
    ) -> Result<(), DeleteTopicsMachineError> {
        if self.plan.topics().len() != outcomes.len() {
            return Err(DeleteTopicsMachineError::OutcomeCountMismatch);
        }
        if self
            .plan
            .topics()
            .iter()
            .zip(outcomes)
            .any(|(topic, outcome)| topic != outcome.topic())
        {
            return Err(DeleteTopicsMachineError::OutcomeTopicMismatch);
        }
        Ok(())
    }

    fn finish(&mut self, terminal: DeleteTopicsTerminal) -> DeleteTopicsTransition {
        self.state = DeleteTopicsState::Completed;
        DeleteTopicsTransition::one(DeleteTopicsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
