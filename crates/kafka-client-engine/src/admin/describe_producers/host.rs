//! Bounded ownership of accepted Admin `DescribeProducers` machines and leader calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    AdminDescribeProducerTarget, AdminDescribeProducersEffect, AdminDescribeProducersInput,
    AdminDescribeProducersMachine, AdminDescribeProducersTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeProducersPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeProducersCall, DescribeProducersRawTerminal},
};

use super::{AdminDescribeProducersHostError, AdminDescribeProducersObserver};

pub(crate) const ADMIN_DESCRIBE_PRODUCERS_CAPACITY: usize = 16;
pub(crate) const ADMIN_DESCRIBE_PRODUCERS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AdminDescribeProducersAdmission {
    pub(crate) observer: AdminDescribeProducersObserver,
    pub(crate) fault: Option<AdminDescribeProducersHostError>,
}

/// One exact target ready for the engine's driver-admission stage.
pub(crate) struct AdminDescribeProducersSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    target: AdminDescribeProducerTarget,
    broker_id: Option<i32>,
}

impl AdminDescribeProducersSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        AdminDescribeProducerTarget,
        Option<i32>,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.target,
            self.broker_id,
        )
    }
}

pub(crate) enum AdminDescribeProducersTurn {
    Idle,
    Progress,
    Submit(AdminDescribeProducersSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdminDescribeProducersHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct AdminDescribeProducersOperation {
    operation_id: OperationId,
    machine: AdminDescribeProducersMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<AdminDescribeProducersSubmission>,
    handoff: AdminDescribeProducersHandoff,
    call: Option<DescribeProducersCall>,
    recovered_call: Option<crate::driver::RecoveredDescribeProducersCall>,
    raw_terminal: Option<DescribeProducersRawTerminal>,
    terminal: Option<AdminDescribeProducersTerminal>,
}

pub(crate) struct AdminDescribeProducersHost {
    operations: Vec<AdminDescribeProducersOperation>,
    completions:
        CompletionRegistry<AdminDescribeProducersTerminal, AdminDescribeProducersPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AdminDescribeProducersHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AdminDescribeProducersHost {
    pub(crate) fn new(publisher: AdminDescribeProducersPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ADMIN_DESCRIBE_PRODUCERS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ADMIN_DESCRIBE_PRODUCERS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ADMIN_DESCRIBE_PRODUCERS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AdminDescribeProducersTurn, AdminDescribeProducersHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AdminDescribeProducersTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AdminDescribeProducersTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, AdminDescribeProducersInput::DeadlineElapsed)?;
            return Ok(AdminDescribeProducersTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AdminDescribeProducersHostError::MissingSubmission)?;
        self.operations[index].handoff = AdminDescribeProducersHandoff::HandedOff;
        Ok(AdminDescribeProducersTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeProducersCall,
    ) -> Result<(), AdminDescribeProducersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminDescribeProducersHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminDescribeProducersHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(AdminDescribeProducersHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, AdminDescribeProducersInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), AdminDescribeProducersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminDescribeProducersHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminDescribeProducersHandoff::HandedOff {
            return Err(AdminDescribeProducersHostError::InvalidHandoff);
        }
        self.apply(operation_id, AdminDescribeProducersInput::DriverRejected)
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
        input: AdminDescribeProducersInput,
    ) -> Result<(), AdminDescribeProducersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminDescribeProducersHostError::UnknownOperation)?;
        let accepted = matches!(&input, AdminDescribeProducersInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AdminDescribeProducersHandoff::HandedOff {
            return Err(AdminDescribeProducersHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AdminDescribeProducersHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: AdminDescribeProducersEffect,
    ) -> Result<(), AdminDescribeProducersHostError> {
        match effect {
            AdminDescribeProducersEffect::Submit {
                operation_id,
                deadline,
                target,
                broker_id,
            } if operation_id == self.operations[index].operation_id
                && deadline == self.operations[index].deadline.core() =>
            {
                self.operations[index].submission = Some(AdminDescribeProducersSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    target,
                    broker_id,
                });
                self.operations[index].handoff = AdminDescribeProducersHandoff::Untouched;
                Ok(())
            }
            AdminDescribeProducersEffect::Complete {
                operation_id,
                terminal,
            } if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(AdminDescribeProducersHostError::SubmissionMismatch),
        }
    }
}
