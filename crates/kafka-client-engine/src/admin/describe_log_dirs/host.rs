//! Bounded ownership of accepted Admin `DescribeLogDirs` machines and calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    AdminDescribeLogDirsEffect, AdminDescribeLogDirsInput, AdminDescribeLogDirsMachine,
    AdminDescribeLogDirsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeLogDirsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeLogDirsCall, DescribeLogDirsRawTerminal, RecoveredDescribeLogDirsCall},
};

use super::{DescribeLogDirsHostError, DescribeLogDirsObserver};

pub(crate) const DESCRIBE_LOG_DIRS_CAPACITY: usize = 16;
const DESCRIBE_LOG_DIRS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DescribeLogDirsAdmission {
    pub(crate) observer: DescribeLogDirsObserver,
    pub(crate) fault: Option<DescribeLogDirsHostError>,
}

/// One exact broker ready for the engine's driver-admission stage.
pub(crate) struct DescribeLogDirsSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    broker_id: i32,
}

impl DescribeLogDirsSubmission {
    pub(crate) const fn into_parts(self) -> (OperationId, OperationDeadline, i32) {
        (self.operation_id, self.deadline, self.broker_id)
    }
}

pub(crate) enum DescribeLogDirsTurn {
    Idle,
    Progress,
    Submit(DescribeLogDirsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DescribeLogDirsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct DescribeLogDirsOperation {
    operation_id: OperationId,
    machine: AdminDescribeLogDirsMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<DescribeLogDirsSubmission>,
    handoff: DescribeLogDirsHandoff,
    call: Option<DescribeLogDirsCall>,
    // Driver-shutdown proof remains live until core accepts the terminal fact.
    recovered_call: Option<RecoveredDescribeLogDirsCall>,
    raw_terminal: Option<DescribeLogDirsRawTerminal>,
    terminal: Option<AdminDescribeLogDirsTerminal>,
}

pub(crate) struct DescribeLogDirsHost {
    operations: Vec<DescribeLogDirsOperation>,
    completions: CompletionRegistry<AdminDescribeLogDirsTerminal, AdminDescribeLogDirsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeLogDirsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeLogDirsHost {
    pub(crate) fn new(publisher: AdminDescribeLogDirsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_LOG_DIRS_CAPACITY),
            completions: CompletionRegistry::with_publisher(DESCRIBE_LOG_DIRS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_LOG_DIRS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeLogDirsTurn, DescribeLogDirsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeLogDirsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeLogDirsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, AdminDescribeLogDirsInput::DeadlineElapsed)?;
            return Ok(DescribeLogDirsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeLogDirsHostError::MissingSubmission)?;
        self.operations[index].handoff = DescribeLogDirsHandoff::HandedOff;
        Ok(DescribeLogDirsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeLogDirsCall,
    ) -> Result<(), DescribeLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeLogDirsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeLogDirsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].terminal.is_some()
        {
            return Err(DescribeLogDirsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, AdminDescribeLogDirsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), DescribeLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeLogDirsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeLogDirsHandoff::HandedOff {
            return Err(DescribeLogDirsHostError::InvalidHandoff);
        }
        self.apply(operation_id, AdminDescribeLogDirsInput::DriverRejected)
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
        input: AdminDescribeLogDirsInput,
    ) -> Result<(), DescribeLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeLogDirsHostError::UnknownOperation)?;
        let accepted = matches!(&input, AdminDescribeLogDirsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DescribeLogDirsHandoff::HandedOff {
            return Err(DescribeLogDirsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DescribeLogDirsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: AdminDescribeLogDirsEffect,
    ) -> Result<(), DescribeLogDirsHostError> {
        let operation_id = self.operations[index].operation_id;
        match effect {
            AdminDescribeLogDirsEffect::Submit {
                operation_id: effect_id,
                deadline,
                broker_id,
            } => {
                if effect_id != operation_id || deadline != self.operations[index].deadline.core() {
                    return Err(DescribeLogDirsHostError::SubmissionMismatch);
                }
                self.operations[index].submission = Some(DescribeLogDirsSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    broker_id,
                });
                self.operations[index].handoff = DescribeLogDirsHandoff::Untouched;
                Ok(())
            }
            AdminDescribeLogDirsEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(DescribeLogDirsHostError::SubmissionMismatch);
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
