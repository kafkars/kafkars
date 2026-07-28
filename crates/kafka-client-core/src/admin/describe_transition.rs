//! Atomic `DescribeCluster` lifecycle transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    DescribeClusterEffect, DescribeClusterFailure, DescribeClusterFailureKind,
    DescribeClusterInput, DescribeClusterMachine, DescribeClusterMachineError,
    DescribeClusterState, DescribeClusterTerminal, DescribeClusterTransition,
};

impl DescribeClusterMachine {
    /// Applies one normalized fact without performing I/O.
    pub fn apply(
        &mut self,
        input: DescribeClusterInput,
    ) -> Result<DescribeClusterTransition, DescribeClusterMachineError> {
        if self.state == DescribeClusterState::Completed {
            return Err(DescribeClusterMachineError::AlreadyCompleted);
        }
        match input {
            DescribeClusterInput::Start { now } => self.start(now),
            DescribeClusterInput::DriverAccepted => self.driver_accepted(),
            DescribeClusterInput::DriverRejected => self.finish_failure(
                DescribeClusterFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DescribeClusterInput::DeadlineElapsed => self.finish_failure(
                DescribeClusterFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DescribeClusterInput::BrokerResponded { description } => {
                self.finish_submitted(DescribeClusterTerminal::Cluster(description))
            }
            DescribeClusterInput::BrokerRejected { error } => {
                self.finish_submitted(DescribeClusterTerminal::BrokerRejected(error))
            }
            DescribeClusterInput::TransportFailed { delivery } => {
                self.finish_failure(DescribeClusterFailureKind::Transport, delivery)
            }
            DescribeClusterInput::ProtocolIncompatible { delivery } => {
                self.finish_failure(DescribeClusterFailureKind::Compatibility, delivery)
            }
            DescribeClusterInput::AuthenticationFailed { delivery } => {
                self.finish_failure(DescribeClusterFailureKind::Authentication, delivery)
            }
            DescribeClusterInput::InvalidResponse => self.finish_failure(
                DescribeClusterFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeClusterTransition, DescribeClusterMachineError> {
        if self.state != DescribeClusterState::Ready {
            return Err(DescribeClusterMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(DescribeClusterTerminal::Failed(
                DescribeClusterFailure::new(
                    DescribeClusterFailureKind::DeadlineElapsed,
                    DeliveryStatus::NotSent,
                ),
            )));
        }
        self.state = DescribeClusterState::AwaitingDriver;
        Ok(DescribeClusterTransition::one(
            DescribeClusterEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                include_fenced_brokers: self.include_fenced_brokers,
                include_authorized_operations: self.include_authorized_operations,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DescribeClusterTransition, DescribeClusterMachineError> {
        if self.state != DescribeClusterState::AwaitingDriver {
            return Err(DescribeClusterMachineError::InvalidState);
        }
        self.state = DescribeClusterState::Submitted;
        Ok(DescribeClusterTransition::none())
    }

    fn finish_failure(
        &mut self,
        kind: DescribeClusterFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeClusterTransition, DescribeClusterMachineError> {
        let expected = match kind {
            DescribeClusterFailureKind::DeadlineElapsed
            | DescribeClusterFailureKind::DriverRejected => DescribeClusterState::AwaitingDriver,
            DescribeClusterFailureKind::Transport
            | DescribeClusterFailureKind::Compatibility
            | DescribeClusterFailureKind::Authentication
            | DescribeClusterFailureKind::InvalidResponse => DescribeClusterState::Submitted,
        };
        if self.state != expected {
            return Err(DescribeClusterMachineError::InvalidState);
        }
        Ok(self.finish(DescribeClusterTerminal::Failed(
            DescribeClusterFailure::new(kind, delivery),
        )))
    }

    fn finish_submitted(
        &mut self,
        terminal: DescribeClusterTerminal,
    ) -> Result<DescribeClusterTransition, DescribeClusterMachineError> {
        if self.state != DescribeClusterState::Submitted {
            return Err(DescribeClusterMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish(&mut self, terminal: DescribeClusterTerminal) -> DescribeClusterTransition {
        self.state = DescribeClusterState::Completed;
        DescribeClusterTransition::one(DescribeClusterEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
