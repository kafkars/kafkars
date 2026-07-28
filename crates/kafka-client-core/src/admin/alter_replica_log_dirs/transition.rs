//! Atomic exact-broker mutation iteration and terminal assignment.

use crate::DeliveryStatus;

use super::{
    AlterReplicaLogDirAssignment, AlterReplicaLogDirOutcome, AlterReplicaLogDirsEffect,
    AlterReplicaLogDirsFailureKind, AlterReplicaLogDirsInput, AlterReplicaLogDirsMachine,
    AlterReplicaLogDirsMachineError, AlterReplicaLogDirsState, AlterReplicaLogDirsTransition,
};

impl AlterReplicaLogDirsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AlterReplicaLogDirsInput,
    ) -> Result<AlterReplicaLogDirsTransition, AlterReplicaLogDirsMachineError> {
        if self.state == AlterReplicaLogDirsState::Completed {
            return Err(AlterReplicaLogDirsMachineError::AlreadyCompleted);
        }
        match input {
            AlterReplicaLogDirsInput::Start { now } => self.start(now),
            AlterReplicaLogDirsInput::DriverAccepted => self.driver_accepted(),
            AlterReplicaLogDirsInput::DriverRejected => self.finish_awaiting(
                AlterReplicaLogDirsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            AlterReplicaLogDirsInput::DeadlineElapsed => self.finish_awaiting(
                AlterReplicaLogDirsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            AlterReplicaLogDirsInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(AlterReplicaLogDirsFailureKind::DeadlineElapsed, delivery)
            }
            AlterReplicaLogDirsInput::BrokerResponded {
                throttle_time_ms,
                outcomes,
            } => self.broker_responded(throttle_time_ms, outcomes),
            AlterReplicaLogDirsInput::ResponseTooLarge => self.finish_submitted(
                AlterReplicaLogDirsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AlterReplicaLogDirsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(AlterReplicaLogDirsFailureKind::Compatibility, delivery)
            }
            AlterReplicaLogDirsInput::TransportFailed { delivery } => {
                self.finish_submitted(AlterReplicaLogDirsFailureKind::Transport, delivery)
            }
            AlterReplicaLogDirsInput::InvalidResponse => self.finish_submitted(
                AlterReplicaLogDirsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AlterReplicaLogDirsTransition, AlterReplicaLogDirsMachineError> {
        if self.state != AlterReplicaLogDirsState::Ready {
            return Err(AlterReplicaLogDirsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return self.finish_failure(
                AlterReplicaLogDirsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            );
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<AlterReplicaLogDirsTransition, AlterReplicaLogDirsMachineError> {
        let broker_id = self
            .current_broker()
            .ok_or(AlterReplicaLogDirsMachineError::InvalidState)?;
        let assignments = self
            .plan
            .assignments()
            .iter()
            .filter(|assignment| assignment.broker_id() == broker_id)
            .cloned()
            .collect::<Vec<AlterReplicaLogDirAssignment>>();
        if assignments.is_empty() {
            return Err(AlterReplicaLogDirsMachineError::InvalidState);
        }
        self.state = AlterReplicaLogDirsState::AwaitingDriver;
        Ok(AlterReplicaLogDirsTransition::one(
            AlterReplicaLogDirsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                broker_id,
                assignments,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AlterReplicaLogDirsTransition, AlterReplicaLogDirsMachineError> {
        if self.state != AlterReplicaLogDirsState::AwaitingDriver {
            return Err(AlterReplicaLogDirsMachineError::InvalidState);
        }
        self.state = AlterReplicaLogDirsState::Submitted;
        Ok(AlterReplicaLogDirsTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        outcomes: Vec<AlterReplicaLogDirOutcome>,
    ) -> Result<AlterReplicaLogDirsTransition, AlterReplicaLogDirsMachineError> {
        if self.state != AlterReplicaLogDirsState::Submitted {
            return Err(AlterReplicaLogDirsMachineError::InvalidState);
        }
        let broker_id = self
            .current_broker()
            .ok_or(AlterReplicaLogDirsMachineError::InvalidState)?;
        let expected = self
            .plan
            .assignments()
            .iter()
            .filter(|assignment| assignment.broker_id() == broker_id);
        if expected.clone().count() != outcomes.len()
            || expected.zip(&outcomes).any(|(assignment, outcome)| {
                assignment.broker_id() != outcome.broker_id()
                    || assignment.topic() != outcome.topic()
                    || assignment.partition() != outcome.partition()
                    || outcome.is_operation_failure()
            })
        {
            return self.finish_failure(
                AlterReplicaLogDirsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            );
        }
        let mut outcomes = outcomes.into_iter();
        for (caller_index, assignment) in self.plan.assignments().iter().enumerate() {
            if assignment.broker_id() == broker_id {
                let mut outcome = outcomes
                    .next()
                    .ok_or(AlterReplicaLogDirsMachineError::InvalidState)?;
                outcome.assign_caller_index(caller_index);
                self.outcomes.push(outcome);
            }
        }
        if outcomes.next().is_some() {
            return Err(AlterReplicaLogDirsMachineError::InvalidState);
        }
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.next_broker += 1;
        if self.next_broker == self.plan.broker_ids().len() {
            return self.finish_batch();
        }
        self.submit_current()
    }
}
