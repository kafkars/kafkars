//! Atomic metadata-quorum transitions and sole terminal assignment.

use crate::DeliveryStatus;

use super::{
    DescribeMetadataQuorumEffect, DescribeMetadataQuorumFailure, DescribeMetadataQuorumFailureKind,
    DescribeMetadataQuorumInput, DescribeMetadataQuorumMachine, DescribeMetadataQuorumMachineError,
    DescribeMetadataQuorumState, DescribeMetadataQuorumTerminal, DescribeMetadataQuorumTransition,
};

impl DescribeMetadataQuorumMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DescribeMetadataQuorumInput,
    ) -> Result<DescribeMetadataQuorumTransition, DescribeMetadataQuorumMachineError> {
        if self.state == DescribeMetadataQuorumState::Completed {
            return Err(DescribeMetadataQuorumMachineError::AlreadyCompleted);
        }
        match input {
            DescribeMetadataQuorumInput::Start { now } => self.start(now),
            DescribeMetadataQuorumInput::DriverAccepted => self.driver_accepted(),
            DescribeMetadataQuorumInput::DriverRejected => self.finish_awaiting(
                DescribeMetadataQuorumFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DescribeMetadataQuorumInput::DeadlineElapsed => self.finish_awaiting(
                DescribeMetadataQuorumFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DescribeMetadataQuorumInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(DescribeMetadataQuorumFailureKind::DeadlineElapsed, delivery)
            }
            DescribeMetadataQuorumInput::BrokerResponded { description } => self
                .finish_submitted_terminal(DescribeMetadataQuorumTerminal::Described(description)),
            DescribeMetadataQuorumInput::BrokerRejected { error } => self
                .finish_submitted_terminal(DescribeMetadataQuorumTerminal::BrokerRejected(error)),
            DescribeMetadataQuorumInput::PartitionRejected { error } => self
                .finish_submitted_terminal(DescribeMetadataQuorumTerminal::PartitionRejected(
                    error,
                )),
            DescribeMetadataQuorumInput::ResponseTooLarge => self.finish_submitted(
                DescribeMetadataQuorumFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DescribeMetadataQuorumInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(DescribeMetadataQuorumFailureKind::Compatibility, delivery)
            }
            DescribeMetadataQuorumInput::TransportFailed { delivery } => {
                self.finish_submitted(DescribeMetadataQuorumFailureKind::Transport, delivery)
            }
            DescribeMetadataQuorumInput::InvalidResponse => self.finish_submitted(
                DescribeMetadataQuorumFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeMetadataQuorumTransition, DescribeMetadataQuorumMachineError> {
        if self.state != DescribeMetadataQuorumState::Ready {
            return Err(DescribeMetadataQuorumMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DescribeMetadataQuorumFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = DescribeMetadataQuorumState::AwaitingDriver;
        Ok(DescribeMetadataQuorumTransition::one(
            DescribeMetadataQuorumEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DescribeMetadataQuorumTransition, DescribeMetadataQuorumMachineError> {
        if self.state != DescribeMetadataQuorumState::AwaitingDriver {
            return Err(DescribeMetadataQuorumMachineError::InvalidState);
        }
        self.state = DescribeMetadataQuorumState::Submitted;
        Ok(DescribeMetadataQuorumTransition::none())
    }

    fn finish_awaiting(
        &mut self,
        kind: DescribeMetadataQuorumFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeMetadataQuorumTransition, DescribeMetadataQuorumMachineError> {
        if self.state != DescribeMetadataQuorumState::AwaitingDriver {
            return Err(DescribeMetadataQuorumMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: DescribeMetadataQuorumFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeMetadataQuorumTransition, DescribeMetadataQuorumMachineError> {
        if self.state != DescribeMetadataQuorumState::Submitted {
            return Err(DescribeMetadataQuorumMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: DescribeMetadataQuorumTerminal,
    ) -> Result<DescribeMetadataQuorumTransition, DescribeMetadataQuorumMachineError> {
        if self.state != DescribeMetadataQuorumState::Submitted {
            return Err(DescribeMetadataQuorumMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: DescribeMetadataQuorumFailureKind,
        delivery: DeliveryStatus,
    ) -> DescribeMetadataQuorumTransition {
        self.finish(DescribeMetadataQuorumTerminal::Failed(
            DescribeMetadataQuorumFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: DescribeMetadataQuorumTerminal,
    ) -> DescribeMetadataQuorumTransition {
        self.state = DescribeMetadataQuorumState::Completed;
        DescribeMetadataQuorumTransition::one(DescribeMetadataQuorumEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
