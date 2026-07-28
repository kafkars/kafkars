//! Atomic Admin `ListOffsets` transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    AdminListOffsetOutcome, AdminListOffsetsBatch, AdminListOffsetsEffect, AdminListOffsetsFailure,
    AdminListOffsetsFailureKind, AdminListOffsetsInput, AdminListOffsetsMachine,
    AdminListOffsetsMachineError, AdminListOffsetsState, AdminListOffsetsTerminal,
    AdminListOffsetsTransition,
};

impl AdminListOffsetsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AdminListOffsetsInput,
    ) -> Result<AdminListOffsetsTransition, AdminListOffsetsMachineError> {
        if self.state == AdminListOffsetsState::Completed {
            return Err(AdminListOffsetsMachineError::AlreadyCompleted);
        }
        match input {
            AdminListOffsetsInput::Start { now } => self.start(now),
            AdminListOffsetsInput::DriverAccepted => self.driver_accepted(),
            AdminListOffsetsInput::DriverRejected => self.finish_awaiting(
                AdminListOffsetsFailureKind::DriverRejected,
                self.current_unsent_delivery(),
            ),
            AdminListOffsetsInput::DeadlineElapsed => self.finish_awaiting(
                AdminListOffsetsFailureKind::DeadlineElapsed,
                self.current_unsent_delivery(),
            ),
            AdminListOffsetsInput::DriverDeadlineElapsed { delivery } => self.finish_submitted(
                AdminListOffsetsFailureKind::DeadlineElapsed,
                self.aggregate_delivery(delivery),
            ),
            AdminListOffsetsInput::BrokerResponded {
                throttle_time_ms,
                outcome,
            } => self.broker_responded(throttle_time_ms, outcome),
            AdminListOffsetsInput::ResponseTooLarge => self.finish_submitted(
                AdminListOffsetsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AdminListOffsetsInput::ProtocolIncompatible { delivery } => self.finish_submitted(
                AdminListOffsetsFailureKind::Compatibility,
                self.aggregate_delivery(delivery),
            ),
            AdminListOffsetsInput::TransportFailed { delivery } => self.finish_submitted(
                AdminListOffsetsFailureKind::Transport,
                self.aggregate_delivery(delivery),
            ),
            AdminListOffsetsInput::InvalidResponse => self.finish_submitted(
                AdminListOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AdminListOffsetsTransition, AdminListOffsetsMachineError> {
        if self.state != AdminListOffsetsState::Ready {
            return Err(AdminListOffsetsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AdminListOffsetsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<AdminListOffsetsTransition, AdminListOffsetsMachineError> {
        let Some(target) = self.plan.targets().get(self.next_target).cloned() else {
            return Err(AdminListOffsetsMachineError::InvalidState);
        };
        self.state = AdminListOffsetsState::AwaitingDriver;
        Ok(AdminListOffsetsTransition::one(
            AdminListOffsetsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                target,
                read_isolation: self.plan.read_isolation(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AdminListOffsetsTransition, AdminListOffsetsMachineError> {
        if self.state != AdminListOffsetsState::AwaitingDriver {
            return Err(AdminListOffsetsMachineError::InvalidState);
        }
        self.state = AdminListOffsetsState::Submitted;
        Ok(AdminListOffsetsTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        outcome: AdminListOffsetOutcome,
    ) -> Result<AdminListOffsetsTransition, AdminListOffsetsMachineError> {
        if self.state != AdminListOffsetsState::Submitted {
            return Err(AdminListOffsetsMachineError::InvalidState);
        }
        let Some(target) = self.plan.targets().get(self.next_target) else {
            return Err(AdminListOffsetsMachineError::InvalidState);
        };
        if target.topic() != outcome.topic() || target.partition() != outcome.partition() {
            return Ok(self.finish_failure(
                AdminListOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        self.outcomes.push(outcome);
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.next_target += 1;
        if self.next_target == self.plan.targets().len() {
            let outcomes = core::mem::take(&mut self.outcomes);
            let batch = AdminListOffsetsBatch::new(self.maximum_throttle_time_ms, outcomes);
            return Ok(self.finish(AdminListOffsetsTerminal::Listed(batch)));
        }
        self.submit_current()
    }

    fn finish_awaiting(
        &mut self,
        kind: AdminListOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminListOffsetsTransition, AdminListOffsetsMachineError> {
        if self.state != AdminListOffsetsState::AwaitingDriver {
            return Err(AdminListOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: AdminListOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminListOffsetsTransition, AdminListOffsetsMachineError> {
        if self.state != AdminListOffsetsState::Submitted {
            return Err(AdminListOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    const fn current_unsent_delivery(&self) -> DeliveryStatus {
        if self.next_target == 0 {
            DeliveryStatus::NotSent
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    const fn aggregate_delivery(&self, current: DeliveryStatus) -> DeliveryStatus {
        if self.next_target == 0 {
            current
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    fn finish_failure(
        &mut self,
        kind: AdminListOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> AdminListOffsetsTransition {
        self.finish(AdminListOffsetsTerminal::Failed(
            AdminListOffsetsFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: AdminListOffsetsTerminal) -> AdminListOffsetsTransition {
        self.state = AdminListOffsetsState::Completed;
        AdminListOffsetsTransition::one(AdminListOffsetsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
