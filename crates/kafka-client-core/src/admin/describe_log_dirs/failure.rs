//! Caller-ordered partial-result settlement for broker operation failures.

use crate::DeliveryStatus;

use super::{
    AdminDescribeLogDirsBatch, AdminDescribeLogDirsBrokerOutcome, AdminDescribeLogDirsEffect,
    AdminDescribeLogDirsFailure, AdminDescribeLogDirsFailureKind, AdminDescribeLogDirsMachine,
    AdminDescribeLogDirsMachineError, AdminDescribeLogDirsState, AdminDescribeLogDirsTerminal,
    AdminDescribeLogDirsTransition,
};

impl AdminDescribeLogDirsMachine {
    pub(super) fn finish_awaiting(
        &mut self,
        kind: AdminDescribeLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminDescribeLogDirsTransition, AdminDescribeLogDirsMachineError> {
        if self.state != AdminDescribeLogDirsState::AwaitingDriver {
            return Err(AdminDescribeLogDirsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    pub(super) fn finish_submitted(
        &mut self,
        kind: AdminDescribeLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminDescribeLogDirsTransition, AdminDescribeLogDirsMachineError> {
        if self.state != AdminDescribeLogDirsState::Submitted {
            return Err(AdminDescribeLogDirsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    pub(super) fn finish_failure(
        &mut self,
        kind: AdminDescribeLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> AdminDescribeLogDirsTransition {
        let current = self.plan.broker_ids()[self.next_broker];
        self.outcomes
            .push(AdminDescribeLogDirsBrokerOutcome::operation_failed(
                current,
                AdminDescribeLogDirsFailure::new(kind, delivery),
            ));
        for broker_id in &self.plan.broker_ids()[self.next_broker + 1..] {
            self.outcomes
                .push(AdminDescribeLogDirsBrokerOutcome::operation_failed(
                    *broker_id,
                    AdminDescribeLogDirsFailure::new(
                        AdminDescribeLogDirsFailureKind::NotAttempted,
                        DeliveryStatus::NotSent,
                    ),
                ));
        }
        let outcomes = core::mem::take(&mut self.outcomes);
        self.finish(AdminDescribeLogDirsTerminal::Described(
            AdminDescribeLogDirsBatch::new(self.maximum_throttle_time_ms, outcomes),
        ))
    }

    pub(super) fn finish(
        &mut self,
        terminal: AdminDescribeLogDirsTerminal,
    ) -> AdminDescribeLogDirsTransition {
        self.state = AdminDescribeLogDirsState::Completed;
        AdminDescribeLogDirsTransition::one(AdminDescribeLogDirsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
