//! Bounded ownership of accepted Admin `AlterReplicaLogDirs` machines and calls.

mod admission;
mod model;
mod recovery;
mod response;
mod terminal;

#[cfg(test)]
mod ownership_test;
#[cfg(test)]
mod response_test;

use kafka_client_core::{
    AlterReplicaLogDirsEffect, AlterReplicaLogDirsInput, AlterReplicaLogDirsTerminal, Moment,
    OperationId,
};

use crate::{
    admin::AdminAlterReplicaLogDirsPublisher,
    completion::{CompletionId, CompletionRegistry},
};

use super::AlterReplicaLogDirsHostError;

pub(crate) use model::{
    AlterReplicaLogDirsAdmission, AlterReplicaLogDirsSubmission, AlterReplicaLogDirsTurn,
};
use model::{
    AlterReplicaLogDirsAttempt, AlterReplicaLogDirsAttemptBounds, AlterReplicaLogDirsHandoff,
    AlterReplicaLogDirsOperation,
};

pub(crate) const ALTER_REPLICA_LOG_DIRS_CAPACITY: usize = 16;
const ALTER_REPLICA_LOG_DIRS_RETAINED_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct AlterReplicaLogDirsHost {
    operations: Vec<AlterReplicaLogDirsOperation>,
    completions: CompletionRegistry<AlterReplicaLogDirsTerminal, AdminAlterReplicaLogDirsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AlterReplicaLogDirsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AlterReplicaLogDirsHost {
    pub(crate) fn new(publisher: AdminAlterReplicaLogDirsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ALTER_REPLICA_LOG_DIRS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ALTER_REPLICA_LOG_DIRS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ALTER_REPLICA_LOG_DIRS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AlterReplicaLogDirsTurn, AlterReplicaLogDirsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AlterReplicaLogDirsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AlterReplicaLogDirsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, AlterReplicaLogDirsInput::DeadlineElapsed)?;
            return Ok(AlterReplicaLogDirsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AlterReplicaLogDirsHostError::MissingSubmission)?;
        self.operations[index].handoff = AlterReplicaLogDirsHandoff::HandedOff;
        Ok(AlterReplicaLogDirsTurn::Submit(submission))
    }

    pub(crate) fn close_admission(&mut self) {
        self.accepting = false;
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.operations.len()
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.operations
            .iter()
            .filter(|operation| operation.submission.is_some())
            .map(|operation| operation.deadline.core())
            .min()
    }

    fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .iter()
            .position(|operation| operation.operation_id == operation_id)
    }

    fn apply(
        &mut self,
        operation_id: OperationId,
        input: AlterReplicaLogDirsInput,
    ) -> Result<(), AlterReplicaLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterReplicaLogDirsHostError::UnknownOperation)?;
        let accepted = matches!(&input, AlterReplicaLogDirsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AlterReplicaLogDirsHandoff::HandedOff {
            return Err(AlterReplicaLogDirsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AlterReplicaLogDirsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: AlterReplicaLogDirsEffect,
    ) -> Result<(), AlterReplicaLogDirsHostError> {
        let operation_id = self.operations[index].operation_id;
        match effect {
            AlterReplicaLogDirsEffect::Submit {
                operation_id: effect_id,
                deadline,
                broker_id,
                assignments,
            } => {
                if effect_id != operation_id
                    || deadline != self.operations[index].deadline.core()
                    || self.operations[index].machine.current_broker() != Some(broker_id)
                    || assignments.is_empty()
                    || assignments
                        .iter()
                        .any(|assignment| assignment.broker_id() != broker_id)
                {
                    return Err(AlterReplicaLogDirsHostError::SubmissionMismatch);
                }
                let bounds = AlterReplicaLogDirsAttemptBounds {
                    request_scratch_limit: self.operations[index].remaining_result_bytes,
                    result_limit: self.operations[index].remaining_result_bytes,
                };
                self.operations[index].attempt = Some(AlterReplicaLogDirsAttempt {
                    broker_id,
                    assignments: assignments.clone(),
                    bounds,
                });
                self.operations[index].submission = Some(AlterReplicaLogDirsSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    broker_id,
                    assignments,
                    bounds,
                });
                self.operations[index].handoff = AlterReplicaLogDirsHandoff::Untouched;
                Ok(())
            }
            AlterReplicaLogDirsEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(AlterReplicaLogDirsHostError::SubmissionMismatch);
                }
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
        }
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
