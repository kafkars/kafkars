//! Atomic reassignment transitions and single terminal assignment.

use crate::DeliveryStatus;

use super::{
    AlterPartitionReassignmentOutcome, AlterPartitionReassignmentsBatch,
    AlterPartitionReassignmentsEffect, AlterPartitionReassignmentsFailure,
    AlterPartitionReassignmentsFailureKind, AlterPartitionReassignmentsInput,
    AlterPartitionReassignmentsMachine, AlterPartitionReassignmentsMachineError,
    AlterPartitionReassignmentsState, AlterPartitionReassignmentsTerminal,
    AlterPartitionReassignmentsTransition,
};

impl AlterPartitionReassignmentsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AlterPartitionReassignmentsInput,
    ) -> Result<AlterPartitionReassignmentsTransition, AlterPartitionReassignmentsMachineError>
    {
        if self.state == AlterPartitionReassignmentsState::Completed {
            return Err(AlterPartitionReassignmentsMachineError::AlreadyCompleted);
        }
        match input {
            AlterPartitionReassignmentsInput::Start { now } => self.start(now),
            AlterPartitionReassignmentsInput::DriverAccepted => self.driver_accepted(),
            AlterPartitionReassignmentsInput::DriverRejected => self.finish_awaiting(
                AlterPartitionReassignmentsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            AlterPartitionReassignmentsInput::DeadlineElapsed => self.finish_awaiting(
                AlterPartitionReassignmentsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            AlterPartitionReassignmentsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    AlterPartitionReassignmentsFailureKind::DeadlineElapsed,
                    delivery,
                ),
            AlterPartitionReassignmentsInput::BrokerResponded { batch } => {
                self.broker_responded(batch)
            }
            AlterPartitionReassignmentsInput::BrokerRejected { error } => self.finish_submitted(
                AlterPartitionReassignmentsFailureKind::Broker(error),
                DeliveryStatus::PossiblySent,
            ),
            AlterPartitionReassignmentsInput::ResponseTooLarge => self.finish_submitted(
                AlterPartitionReassignmentsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AlterPartitionReassignmentsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    AlterPartitionReassignmentsFailureKind::Compatibility,
                    delivery,
                ),
            AlterPartitionReassignmentsInput::TransportFailed { delivery } => {
                self.finish_submitted(AlterPartitionReassignmentsFailureKind::Transport, delivery)
            }
            AlterPartitionReassignmentsInput::InvalidResponse => self.finish_submitted(
                AlterPartitionReassignmentsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AlterPartitionReassignmentsTransition, AlterPartitionReassignmentsMachineError>
    {
        if self.state != AlterPartitionReassignmentsState::Ready {
            return Err(AlterPartitionReassignmentsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AlterPartitionReassignmentsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = AlterPartitionReassignmentsState::AwaitingDriver;
        Ok(AlterPartitionReassignmentsTransition::one(
            AlterPartitionReassignmentsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AlterPartitionReassignmentsTransition, AlterPartitionReassignmentsMachineError>
    {
        if self.state != AlterPartitionReassignmentsState::AwaitingDriver {
            return Err(AlterPartitionReassignmentsMachineError::InvalidState);
        }
        self.state = AlterPartitionReassignmentsState::Submitted;
        Ok(AlterPartitionReassignmentsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: AlterPartitionReassignmentsBatch,
    ) -> Result<AlterPartitionReassignmentsTransition, AlterPartitionReassignmentsMachineError>
    {
        if self.state != AlterPartitionReassignmentsState::Submitted {
            return Err(AlterPartitionReassignmentsMachineError::InvalidState);
        }
        if !self.outcomes_match_plan(batch.outcomes()) {
            return Ok(self.finish_failure(
                AlterPartitionReassignmentsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        Ok(self.finish(AlterPartitionReassignmentsTerminal::Altered(batch)))
    }

    fn outcomes_match_plan(&self, outcomes: &[AlterPartitionReassignmentOutcome]) -> bool {
        self.plan.changes().len() == outcomes.len()
            && self
                .plan
                .changes()
                .iter()
                .zip(outcomes)
                .all(|(change, outcome)| {
                    change.topic() == outcome.topic() && change.partition() == outcome.partition()
                })
    }

    fn finish_awaiting(
        &mut self,
        kind: AlterPartitionReassignmentsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterPartitionReassignmentsTransition, AlterPartitionReassignmentsMachineError>
    {
        if self.state != AlterPartitionReassignmentsState::AwaitingDriver {
            return Err(AlterPartitionReassignmentsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: AlterPartitionReassignmentsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterPartitionReassignmentsTransition, AlterPartitionReassignmentsMachineError>
    {
        if self.state != AlterPartitionReassignmentsState::Submitted {
            return Err(AlterPartitionReassignmentsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: AlterPartitionReassignmentsFailureKind,
        delivery: DeliveryStatus,
    ) -> AlterPartitionReassignmentsTransition {
        self.finish(AlterPartitionReassignmentsTerminal::Failed(
            AlterPartitionReassignmentsFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: AlterPartitionReassignmentsTerminal,
    ) -> AlterPartitionReassignmentsTransition {
        self.state = AlterPartitionReassignmentsState::Completed;
        AlterPartitionReassignmentsTransition::one(AlterPartitionReassignmentsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
