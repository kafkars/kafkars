//! Atomic `DescribeTopics` lifecycle transitions and terminal single assignment.

mod validation;

#[cfg(test)]
mod topic_id_test;

use super::{
    DescribeTopicIdOutcome, DescribeTopicOutcome, DescribeTopicsEffect, DescribeTopicsFailure,
    DescribeTopicsInput, DescribeTopicsMachine, DescribeTopicsMachineError,
    DescribeTopicsSelection, DescribeTopicsState, DescribeTopicsTerminal, DescribeTopicsTransition,
};

impl DescribeTopicsMachine {
    /// Applies one normalized fact without hidden I/O or retry behavior.
    pub fn apply(
        &mut self,
        input: DescribeTopicsInput,
    ) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state == DescribeTopicsState::Completed {
            return Err(DescribeTopicsMachineError::AlreadyCompleted);
        }
        match input {
            DescribeTopicsInput::Start { now } => self.start(now),
            DescribeTopicsInput::DriverAccepted => self.driver_accepted(),
            DescribeTopicsInput::DriverRejected => self.driver_rejected(),
            DescribeTopicsInput::DeadlineElapsed => self.deadline_elapsed(),
            DescribeTopicsInput::DriverDeadlineElapsed { delivery } => {
                self.driver_deadline_elapsed(delivery)
            }
            DescribeTopicsInput::BrokerResponded { outcomes } => self.broker_responded(outcomes),
            DescribeTopicsInput::BrokerRespondedById { outcomes } => {
                self.broker_responded_by_id(outcomes)
            }
            DescribeTopicsInput::BrokerRejected { code } => self.broker_rejected(code),
            DescribeTopicsInput::ResponseTooLarge => self.response_too_large(),
            DescribeTopicsInput::ProtocolIncompatible => self.protocol_incompatible(),
            DescribeTopicsInput::TransportFailed { delivery } => self.transport_failed(delivery),
            DescribeTopicsInput::InvalidResponse => self.invalid_response(),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::Ready {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(DescribeTopicsTerminal::Failed(
                DescribeTopicsFailure::deadline_elapsed(),
            )));
        }
        self.state = DescribeTopicsState::AwaitingDriver;
        Ok(DescribeTopicsTransition::one(
            DescribeTopicsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(&mut self) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::AwaitingDriver {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        self.state = DescribeTopicsState::Submitted;
        Ok(DescribeTopicsTransition::none())
    }

    fn driver_rejected(&mut self) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::AwaitingDriver {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DescribeTopicsTerminal::Failed(
            DescribeTopicsFailure::driver_rejected(),
        )))
    }

    fn deadline_elapsed(&mut self) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::AwaitingDriver {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DescribeTopicsTerminal::Failed(
            DescribeTopicsFailure::deadline_elapsed(),
        )))
    }

    fn broker_responded(
        &mut self,
        mut outcomes: Vec<DescribeTopicOutcome>,
    ) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::Submitted {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        self.validate_outcomes(&outcomes)?;
        if !self.plan.selection().includes_internal_topics() {
            outcomes.retain(|outcome| !outcome.is_internal());
        }
        Ok(self.finish(DescribeTopicsTerminal::Topics(outcomes)))
    }

    fn broker_responded_by_id(
        &mut self,
        outcomes: Vec<DescribeTopicIdOutcome>,
    ) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::Submitted {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        let DescribeTopicsSelection::Ids(topic_ids) = self.plan.selection() else {
            return Err(DescribeTopicsMachineError::OutcomeSelectionMismatch);
        };
        if topic_ids.len() != outcomes.len() {
            return Err(DescribeTopicsMachineError::OutcomeCountMismatch);
        }
        if topic_ids
            .iter()
            .zip(&outcomes)
            .any(|(topic_id, outcome)| *topic_id != outcome.topic_id())
        {
            return Err(DescribeTopicsMachineError::OutcomeTopicIdMismatch);
        }
        if !self.plan.include_authorized_operations()
            && outcomes
                .iter()
                .any(DescribeTopicIdOutcome::has_authorized_operations)
        {
            return Err(DescribeTopicsMachineError::UnexpectedAuthorizedOperations);
        }
        Ok(self.finish(DescribeTopicsTerminal::TopicIds(outcomes)))
    }

    fn transport_failed(
        &mut self,
        delivery: crate::DeliveryStatus,
    ) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::Submitted {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DescribeTopicsTerminal::Failed(
            DescribeTopicsFailure::transport(delivery),
        )))
    }

    fn broker_rejected(
        &mut self,
        code: core::num::NonZeroI16,
    ) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::Submitted {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DescribeTopicsTerminal::Failed(
            DescribeTopicsFailure::broker(code),
        )))
    }

    fn driver_deadline_elapsed(
        &mut self,
        delivery: crate::DeliveryStatus,
    ) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::Submitted {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DescribeTopicsTerminal::Failed(
            DescribeTopicsFailure::driver_deadline_elapsed(delivery),
        )))
    }

    fn invalid_response(&mut self) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::Submitted {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DescribeTopicsTerminal::Failed(
            DescribeTopicsFailure::invalid_response(),
        )))
    }

    fn response_too_large(
        &mut self,
    ) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::Submitted {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DescribeTopicsTerminal::Failed(
            DescribeTopicsFailure::response_too_large(),
        )))
    }

    fn protocol_incompatible(
        &mut self,
    ) -> Result<DescribeTopicsTransition, DescribeTopicsMachineError> {
        if self.state != DescribeTopicsState::Submitted {
            return Err(DescribeTopicsMachineError::InvalidState);
        }
        Ok(self.finish(DescribeTopicsTerminal::Failed(
            DescribeTopicsFailure::compatibility(),
        )))
    }

    fn finish(&mut self, terminal: DescribeTopicsTerminal) -> DescribeTopicsTransition {
        self.state = DescribeTopicsState::Completed;
        DescribeTopicsTransition::one(DescribeTopicsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
