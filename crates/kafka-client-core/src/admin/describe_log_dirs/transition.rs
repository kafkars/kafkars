//! Atomic exact-broker iteration and terminal assignment.

use crate::DeliveryStatus;

use super::{
    AdminDescribeLogDirsBatch, AdminDescribeLogDirsEffect, AdminDescribeLogDirsFailureKind,
    AdminDescribeLogDirsInput, AdminDescribeLogDirsMachine, AdminDescribeLogDirsMachineError,
    AdminDescribeLogDirsState, AdminDescribeLogDirsTerminal, AdminDescribeLogDirsTransition,
};

impl AdminDescribeLogDirsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AdminDescribeLogDirsInput,
    ) -> Result<AdminDescribeLogDirsTransition, AdminDescribeLogDirsMachineError> {
        if self.state == AdminDescribeLogDirsState::Completed {
            return Err(AdminDescribeLogDirsMachineError::AlreadyCompleted);
        }
        match input {
            AdminDescribeLogDirsInput::Start { now } => self.start(now),
            AdminDescribeLogDirsInput::DriverAccepted => self.driver_accepted(),
            AdminDescribeLogDirsInput::DriverRejected => self.finish_awaiting(
                AdminDescribeLogDirsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            AdminDescribeLogDirsInput::DeadlineElapsed => self.finish_awaiting(
                AdminDescribeLogDirsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            AdminDescribeLogDirsInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(AdminDescribeLogDirsFailureKind::DeadlineElapsed, delivery)
            }
            AdminDescribeLogDirsInput::BrokerResponded {
                throttle_time_ms,
                outcome,
            } => self.broker_responded(throttle_time_ms, outcome),
            AdminDescribeLogDirsInput::ResponseTooLarge => self.finish_submitted(
                AdminDescribeLogDirsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AdminDescribeLogDirsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(AdminDescribeLogDirsFailureKind::Compatibility, delivery)
            }
            AdminDescribeLogDirsInput::TransportFailed { delivery } => {
                self.finish_submitted(AdminDescribeLogDirsFailureKind::Transport, delivery)
            }
            AdminDescribeLogDirsInput::InvalidResponse => self.finish_submitted(
                AdminDescribeLogDirsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AdminDescribeLogDirsTransition, AdminDescribeLogDirsMachineError> {
        if self.state != AdminDescribeLogDirsState::Ready {
            return Err(AdminDescribeLogDirsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AdminDescribeLogDirsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<AdminDescribeLogDirsTransition, AdminDescribeLogDirsMachineError> {
        let Some(broker_id) = self.current_broker() else {
            return Err(AdminDescribeLogDirsMachineError::InvalidState);
        };
        self.state = AdminDescribeLogDirsState::AwaitingDriver;
        Ok(AdminDescribeLogDirsTransition::one(
            AdminDescribeLogDirsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                broker_id,
                selection: self.plan.selection().clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AdminDescribeLogDirsTransition, AdminDescribeLogDirsMachineError> {
        if self.state != AdminDescribeLogDirsState::AwaitingDriver {
            return Err(AdminDescribeLogDirsMachineError::InvalidState);
        }
        self.state = AdminDescribeLogDirsState::Submitted;
        Ok(AdminDescribeLogDirsTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        outcome: super::AdminDescribeLogDirsBrokerOutcome,
    ) -> Result<AdminDescribeLogDirsTransition, AdminDescribeLogDirsMachineError> {
        if self.state != AdminDescribeLogDirsState::Submitted {
            return Err(AdminDescribeLogDirsMachineError::InvalidState);
        }
        if self.current_broker() != Some(outcome.broker_id()) {
            return Ok(self.finish_failure(
                AdminDescribeLogDirsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        self.outcomes.push(outcome);
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.next_broker += 1;
        if self.next_broker == self.plan.broker_ids().len() {
            let outcomes = core::mem::take(&mut self.outcomes);
            return Ok(self.finish(AdminDescribeLogDirsTerminal::Described(
                AdminDescribeLogDirsBatch::new(self.maximum_throttle_time_ms, outcomes),
            )));
        }
        self.submit_current()
    }
}
