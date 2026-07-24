//! Atomic `CreatePartitions` lifecycle transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    CreatePartitionsEffect, CreatePartitionsFailure, CreatePartitionsInput,
    CreatePartitionsMachine, CreatePartitionsMachineError, CreatePartitionsState,
    CreatePartitionsTerminal, CreatePartitionsTransition, PartitionIncreaseOutcome,
};

impl CreatePartitionsMachine {
    /// Applies one normalized fact without hidden I/O or retry behavior.
    pub fn apply(
        &mut self,
        input: CreatePartitionsInput,
    ) -> Result<CreatePartitionsTransition, CreatePartitionsMachineError> {
        if self.state == CreatePartitionsState::Completed {
            return Err(CreatePartitionsMachineError::AlreadyCompleted);
        }
        match input {
            CreatePartitionsInput::Start { now } => self.start(now),
            CreatePartitionsInput::DriverAccepted => self.driver_accepted(),
            CreatePartitionsInput::DriverRejected => self.driver_rejected(),
            CreatePartitionsInput::DeadlineElapsed => self.deadline_elapsed(),
            CreatePartitionsInput::DriverDeadlineElapsed { delivery } => {
                self.driver_deadline_elapsed(delivery)
            }
            CreatePartitionsInput::BrokerResponded { outcomes } => self.broker_responded(outcomes),
            CreatePartitionsInput::TransportFailed { delivery } => self.transport_failed(delivery),
            CreatePartitionsInput::InvalidResponse => self.invalid_response(),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<CreatePartitionsTransition, CreatePartitionsMachineError> {
        if self.state != CreatePartitionsState::Ready {
            return Err(CreatePartitionsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(CreatePartitionsTerminal::Failed(
                CreatePartitionsFailure::deadline_elapsed(),
            )));
        }
        self.state = CreatePartitionsState::AwaitingDriver;
        Ok(CreatePartitionsTransition::one(
            CreatePartitionsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<CreatePartitionsTransition, CreatePartitionsMachineError> {
        if self.state != CreatePartitionsState::AwaitingDriver {
            return Err(CreatePartitionsMachineError::InvalidState);
        }
        self.state = CreatePartitionsState::Submitted;
        Ok(CreatePartitionsTransition::none())
    }

    fn driver_rejected(
        &mut self,
    ) -> Result<CreatePartitionsTransition, CreatePartitionsMachineError> {
        if self.state != CreatePartitionsState::AwaitingDriver {
            return Err(CreatePartitionsMachineError::InvalidState);
        }
        Ok(self.finish(CreatePartitionsTerminal::Failed(
            CreatePartitionsFailure::driver_rejected(),
        )))
    }

    fn deadline_elapsed(
        &mut self,
    ) -> Result<CreatePartitionsTransition, CreatePartitionsMachineError> {
        if self.state != CreatePartitionsState::AwaitingDriver {
            return Err(CreatePartitionsMachineError::InvalidState);
        }
        Ok(self.finish(CreatePartitionsTerminal::Failed(
            CreatePartitionsFailure::deadline_elapsed(),
        )))
    }

    fn broker_responded(
        &mut self,
        outcomes: Vec<PartitionIncreaseOutcome>,
    ) -> Result<CreatePartitionsTransition, CreatePartitionsMachineError> {
        if self.state != CreatePartitionsState::Submitted {
            return Err(CreatePartitionsMachineError::InvalidState);
        }
        self.validate_outcomes(&outcomes)?;
        Ok(self.finish(CreatePartitionsTerminal::Topics(outcomes)))
    }

    fn driver_deadline_elapsed(
        &mut self,
        delivery: DeliveryStatus,
    ) -> Result<CreatePartitionsTransition, CreatePartitionsMachineError> {
        if self.state != CreatePartitionsState::Submitted {
            return Err(CreatePartitionsMachineError::InvalidState);
        }
        Ok(self.finish(CreatePartitionsTerminal::Failed(
            CreatePartitionsFailure::driver_deadline_elapsed(delivery),
        )))
    }

    fn transport_failed(
        &mut self,
        delivery: DeliveryStatus,
    ) -> Result<CreatePartitionsTransition, CreatePartitionsMachineError> {
        if self.state != CreatePartitionsState::Submitted {
            return Err(CreatePartitionsMachineError::InvalidState);
        }
        Ok(self.finish(CreatePartitionsTerminal::Failed(
            CreatePartitionsFailure::transport(delivery),
        )))
    }

    fn invalid_response(
        &mut self,
    ) -> Result<CreatePartitionsTransition, CreatePartitionsMachineError> {
        if self.state != CreatePartitionsState::Submitted {
            return Err(CreatePartitionsMachineError::InvalidState);
        }
        Ok(self.finish(CreatePartitionsTerminal::Failed(
            CreatePartitionsFailure::invalid_response(),
        )))
    }

    fn validate_outcomes(
        &self,
        outcomes: &[PartitionIncreaseOutcome],
    ) -> Result<(), CreatePartitionsMachineError> {
        if self.plan.topics().len() != outcomes.len() {
            return Err(CreatePartitionsMachineError::OutcomeCountMismatch);
        }
        if self
            .plan
            .topics()
            .iter()
            .zip(outcomes)
            .any(|(topic, outcome)| topic.topic() != outcome.topic())
        {
            return Err(CreatePartitionsMachineError::OutcomeTopicMismatch);
        }
        Ok(())
    }

    fn finish(&mut self, terminal: CreatePartitionsTerminal) -> CreatePartitionsTransition {
        self.state = CreatePartitionsState::Completed;
        CreatePartitionsTransition::one(CreatePartitionsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
