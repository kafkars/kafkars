//! Atomic feature-metadata transitions and sole terminal assignment.

use crate::DeliveryStatus;

use super::{
    DescribeFeaturesEffect, DescribeFeaturesFailure, DescribeFeaturesFailureKind,
    DescribeFeaturesInput, DescribeFeaturesMachine, DescribeFeaturesMachineError,
    DescribeFeaturesState, DescribeFeaturesTerminal, DescribeFeaturesTransition,
};

impl DescribeFeaturesMachine {
    /// Applies one normalized fact without hidden I/O, retry, cache, or cancellation.
    pub fn apply(
        &mut self,
        input: DescribeFeaturesInput,
    ) -> Result<DescribeFeaturesTransition, DescribeFeaturesMachineError> {
        if self.state == DescribeFeaturesState::Completed {
            return Err(DescribeFeaturesMachineError::AlreadyCompleted);
        }
        match input {
            DescribeFeaturesInput::Start { now } => self.start(now),
            DescribeFeaturesInput::DriverAccepted => self.driver_accepted(),
            DescribeFeaturesInput::DriverRejected => self.finish_awaiting(
                DescribeFeaturesFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DescribeFeaturesInput::DeadlineElapsed => self.finish_awaiting(
                DescribeFeaturesFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DescribeFeaturesInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(DescribeFeaturesFailureKind::DeadlineElapsed, delivery)
            }
            DescribeFeaturesInput::BrokerResponded { description } => {
                self.finish_submitted_terminal(DescribeFeaturesTerminal::Described(description))
            }
            DescribeFeaturesInput::BrokerRejected { error } => {
                self.finish_submitted_terminal(DescribeFeaturesTerminal::BrokerRejected(error))
            }
            DescribeFeaturesInput::ResponseTooLarge => self.finish_submitted(
                DescribeFeaturesFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DescribeFeaturesInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(DescribeFeaturesFailureKind::Compatibility, delivery)
            }
            DescribeFeaturesInput::TransportFailed { delivery } => {
                self.finish_submitted(DescribeFeaturesFailureKind::Transport, delivery)
            }
            DescribeFeaturesInput::InvalidResponse => self.finish_submitted(
                DescribeFeaturesFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeFeaturesTransition, DescribeFeaturesMachineError> {
        if self.state != DescribeFeaturesState::Ready {
            return Err(DescribeFeaturesMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DescribeFeaturesFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = DescribeFeaturesState::AwaitingDriver;
        Ok(DescribeFeaturesTransition::one(
            DescribeFeaturesEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DescribeFeaturesTransition, DescribeFeaturesMachineError> {
        if self.state != DescribeFeaturesState::AwaitingDriver {
            return Err(DescribeFeaturesMachineError::InvalidState);
        }
        self.state = DescribeFeaturesState::Submitted;
        Ok(DescribeFeaturesTransition::none())
    }

    fn finish_awaiting(
        &mut self,
        kind: DescribeFeaturesFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeFeaturesTransition, DescribeFeaturesMachineError> {
        if self.state != DescribeFeaturesState::AwaitingDriver {
            return Err(DescribeFeaturesMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: DescribeFeaturesFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeFeaturesTransition, DescribeFeaturesMachineError> {
        if self.state != DescribeFeaturesState::Submitted {
            return Err(DescribeFeaturesMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: DescribeFeaturesTerminal,
    ) -> Result<DescribeFeaturesTransition, DescribeFeaturesMachineError> {
        if self.state != DescribeFeaturesState::Submitted {
            return Err(DescribeFeaturesMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: DescribeFeaturesFailureKind,
        delivery: DeliveryStatus,
    ) -> DescribeFeaturesTransition {
        self.finish(DescribeFeaturesTerminal::Failed(
            DescribeFeaturesFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: DescribeFeaturesTerminal) -> DescribeFeaturesTransition {
        self.state = DescribeFeaturesState::Completed;
        DescribeFeaturesTransition::one(DescribeFeaturesEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
