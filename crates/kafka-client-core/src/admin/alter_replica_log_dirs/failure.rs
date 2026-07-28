//! Caller-ordered partial-result settlement for broker-call failures.

use crate::DeliveryStatus;

use super::{
    AlterReplicaLogDirOutcome, AlterReplicaLogDirsBatch, AlterReplicaLogDirsEffect,
    AlterReplicaLogDirsFailure, AlterReplicaLogDirsFailureKind, AlterReplicaLogDirsMachine,
    AlterReplicaLogDirsMachineError, AlterReplicaLogDirsState, AlterReplicaLogDirsTerminal,
    AlterReplicaLogDirsTransition,
};

impl AlterReplicaLogDirsMachine {
    pub(super) fn finish_awaiting(
        &mut self,
        kind: AlterReplicaLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterReplicaLogDirsTransition, AlterReplicaLogDirsMachineError> {
        if self.state != AlterReplicaLogDirsState::AwaitingDriver {
            return Err(AlterReplicaLogDirsMachineError::InvalidState);
        }
        self.finish_failure(kind, delivery)
    }

    pub(super) fn finish_submitted(
        &mut self,
        kind: AlterReplicaLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterReplicaLogDirsTransition, AlterReplicaLogDirsMachineError> {
        if self.state != AlterReplicaLogDirsState::Submitted {
            return Err(AlterReplicaLogDirsMachineError::InvalidState);
        }
        self.finish_failure(kind, delivery)
    }

    pub(super) fn finish_failure(
        &mut self,
        kind: AlterReplicaLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterReplicaLogDirsTransition, AlterReplicaLogDirsMachineError> {
        let current = self
            .current_broker()
            .ok_or(AlterReplicaLogDirsMachineError::InvalidState)?;
        for (caller_index, assignment) in self.plan.assignments().iter().enumerate() {
            if assignment.broker_id() == current {
                let mut outcome = AlterReplicaLogDirOutcome::operation_failed(
                    assignment.broker_id(),
                    assignment.topic().to_owned(),
                    assignment.partition(),
                    AlterReplicaLogDirsFailure::new(kind, delivery),
                );
                outcome.assign_caller_index(caller_index);
                self.outcomes.push(outcome);
            }
        }
        for broker_id in &self.plan.broker_ids()[self.next_broker + 1..] {
            for (caller_index, assignment) in self.plan.assignments().iter().enumerate() {
                if assignment.broker_id() == *broker_id {
                    let mut outcome = AlterReplicaLogDirOutcome::operation_failed(
                        assignment.broker_id(),
                        assignment.topic().to_owned(),
                        assignment.partition(),
                        AlterReplicaLogDirsFailure::new(
                            AlterReplicaLogDirsFailureKind::NotAttempted,
                            DeliveryStatus::NotSent,
                        ),
                    );
                    outcome.assign_caller_index(caller_index);
                    self.outcomes.push(outcome);
                }
            }
        }
        self.finish_batch()
    }

    pub(super) fn finish_batch(
        &mut self,
    ) -> Result<AlterReplicaLogDirsTransition, AlterReplicaLogDirsMachineError> {
        if self.outcomes.len() != self.plan.assignments().len() {
            return Err(AlterReplicaLogDirsMachineError::InvalidState);
        }
        self.outcomes
            .sort_unstable_by_key(AlterReplicaLogDirOutcome::caller_index);
        if self
            .outcomes
            .iter()
            .enumerate()
            .any(|(caller_index, outcome)| outcome.caller_index() != caller_index)
        {
            return Err(AlterReplicaLogDirsMachineError::InvalidState);
        }
        for outcome in &mut self.outcomes {
            outcome.clear_caller_index();
        }
        let outcomes = core::mem::take(&mut self.outcomes);
        Ok(self.finish(AlterReplicaLogDirsTerminal::Altered(
            AlterReplicaLogDirsBatch::new(self.maximum_throttle_time_ms, outcomes),
        )))
    }

    pub(super) fn finish(
        &mut self,
        terminal: AlterReplicaLogDirsTerminal,
    ) -> AlterReplicaLogDirsTransition {
        self.state = AlterReplicaLogDirsState::Completed;
        AlterReplicaLogDirsTransition::one(AlterReplicaLogDirsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
