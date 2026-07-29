//! Bounded ownership of accepted Admin `DescribeReplicaLogDirs` machines and calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DescribeReplicaLogDirsEffect, DescribeReplicaLogDirsInput, DescribeReplicaLogDirsMachine,
    DescribeReplicaLogDirsReplica, DescribeReplicaLogDirsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeReplicaLogDirsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeReplicaLogDirsCall, DescribeReplicaLogDirsRawTerminal},
};

use super::{DescribeReplicaLogDirsHostError, DescribeReplicaLogDirsObserver};

pub(crate) const DESCRIBE_REPLICA_LOG_DIRS_CAPACITY: usize = 16;
const DESCRIBE_REPLICA_LOG_DIRS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DescribeReplicaLogDirsAdmission {
    pub(crate) observer: DescribeReplicaLogDirsObserver,
    pub(crate) fault: Option<DescribeReplicaLogDirsHostError>,
}

/// One exact broker batch ready for request construction and driver admission.
pub(crate) struct DescribeReplicaLogDirsSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    broker_id: i32,
    replicas: Vec<DescribeReplicaLogDirsReplica>,
    request_retained_limit: usize,
}

impl DescribeReplicaLogDirsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        i32,
        Vec<DescribeReplicaLogDirsReplica>,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.broker_id,
            self.replicas,
            self.request_retained_limit,
        )
    }
}

pub(crate) enum DescribeReplicaLogDirsTurn {
    Idle,
    Progress,
    Submit(DescribeReplicaLogDirsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DescribeReplicaLogDirsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct DescribeReplicaLogDirsOperation {
    operation_id: OperationId,
    machine: DescribeReplicaLogDirsMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<DescribeReplicaLogDirsSubmission>,
    current_replicas: Option<Vec<DescribeReplicaLogDirsReplica>>,
    handoff: DescribeReplicaLogDirsHandoff,
    call: Option<DescribeReplicaLogDirsCall>,
    recovered_call: Option<crate::driver::RecoveredDescribeReplicaLogDirsCall>,
    raw_terminal: Option<DescribeReplicaLogDirsRawTerminal>,
    terminal: Option<DescribeReplicaLogDirsTerminal>,
}

pub(crate) struct DescribeReplicaLogDirsHost {
    operations: Vec<DescribeReplicaLogDirsOperation>,
    completions:
        CompletionRegistry<DescribeReplicaLogDirsTerminal, AdminDescribeReplicaLogDirsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeReplicaLogDirsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeReplicaLogDirsHost {
    pub(crate) fn new(publisher: AdminDescribeReplicaLogDirsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_REPLICA_LOG_DIRS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                DESCRIBE_REPLICA_LOG_DIRS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_REPLICA_LOG_DIRS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeReplicaLogDirsTurn, DescribeReplicaLogDirsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeReplicaLogDirsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeReplicaLogDirsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DescribeReplicaLogDirsInput::DeadlineElapsed)?;
            return Ok(DescribeReplicaLogDirsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeReplicaLogDirsHostError::MissingSubmission)?;
        self.operations[index].handoff = DescribeReplicaLogDirsHandoff::HandedOff;
        Ok(DescribeReplicaLogDirsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        replicas: Vec<DescribeReplicaLogDirsReplica>,
        call: DescribeReplicaLogDirsCall,
    ) -> Result<(), DescribeReplicaLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeReplicaLogDirsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeReplicaLogDirsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].current_replicas.is_some()
        {
            return Err(DescribeReplicaLogDirsHostError::InvalidHandoff);
        }
        self.operations[index].current_replicas = Some(replicas);
        self.operations[index].call = Some(call);
        self.apply(operation_id, DescribeReplicaLogDirsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), DescribeReplicaLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeReplicaLogDirsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeReplicaLogDirsHandoff::HandedOff {
            return Err(DescribeReplicaLogDirsHostError::InvalidHandoff);
        }
        self.apply(operation_id, DescribeReplicaLogDirsInput::DriverRejected)
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
        input: DescribeReplicaLogDirsInput,
    ) -> Result<(), DescribeReplicaLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeReplicaLogDirsHostError::UnknownOperation)?;
        let accepted = matches!(&input, DescribeReplicaLogDirsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DescribeReplicaLogDirsHandoff::HandedOff {
            return Err(DescribeReplicaLogDirsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DescribeReplicaLogDirsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: DescribeReplicaLogDirsEffect,
    ) -> Result<(), DescribeReplicaLogDirsHostError> {
        let operation_id = self.operations[index].operation_id;
        match effect {
            DescribeReplicaLogDirsEffect::Submit {
                operation_id: effect_id,
                deadline,
                broker_id,
                replicas,
            } => {
                if effect_id != operation_id || deadline != self.operations[index].deadline.core() {
                    return Err(DescribeReplicaLogDirsHostError::SubmissionMismatch);
                }
                self.operations[index].submission = Some(DescribeReplicaLogDirsSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    broker_id,
                    replicas,
                    request_retained_limit: self.operations[index].remaining_result_bytes,
                });
                self.operations[index].handoff = DescribeReplicaLogDirsHandoff::Untouched;
                Ok(())
            }
            DescribeReplicaLogDirsEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(DescribeReplicaLogDirsHostError::SubmissionMismatch);
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
