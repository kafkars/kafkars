//! Atomic `DescribeTopics` lifecycle transitions and terminal single assignment.

use super::{
    DescribeTopicOutcome, DescribeTopicsEffect, DescribeTopicsFailure, DescribeTopicsInput,
    DescribeTopicsMachine, DescribeTopicsMachineError, DescribeTopicsSelection,
    DescribeTopicsState, DescribeTopicsTerminal, DescribeTopicsTransition,
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

    fn validate_outcomes(
        &self,
        outcomes: &[DescribeTopicOutcome],
    ) -> Result<(), DescribeTopicsMachineError> {
        match self.plan.selection() {
            DescribeTopicsSelection::Named(topics) => {
                if topics.len() != outcomes.len() {
                    return Err(DescribeTopicsMachineError::OutcomeCountMismatch);
                }
                if topics
                    .iter()
                    .zip(outcomes)
                    .any(|(topic, outcome)| topic != outcome.topic())
                {
                    return Err(DescribeTopicsMachineError::OutcomeTopicMismatch);
                }
            }
            DescribeTopicsSelection::All { .. } => validate_all_outcomes(outcomes)?,
        }
        Ok(())
    }

    fn finish(&mut self, terminal: DescribeTopicsTerminal) -> DescribeTopicsTransition {
        self.state = DescribeTopicsState::Completed;
        DescribeTopicsTransition::one(DescribeTopicsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn validate_all_outcomes(
    outcomes: &[DescribeTopicOutcome],
) -> Result<(), DescribeTopicsMachineError> {
    if outcomes.iter().any(|outcome| outcome.topic().is_empty()) {
        return Err(DescribeTopicsMachineError::EmptyOutcomeTopic);
    }
    for pair in outcomes.windows(2) {
        match pair[0].topic().as_bytes().cmp(pair[1].topic().as_bytes()) {
            core::cmp::Ordering::Less => {}
            core::cmp::Ordering::Equal => {
                return Err(DescribeTopicsMachineError::DuplicateOutcomeTopic);
            }
            core::cmp::Ordering::Greater => {
                return Err(DescribeTopicsMachineError::OutcomeTopicOrder);
            }
        }
    }
    Ok(())
}
