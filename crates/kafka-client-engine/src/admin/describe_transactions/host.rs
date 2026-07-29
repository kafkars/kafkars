//! Bounded ownership of accepted Admin `DescribeTransactions` machines and coordinator calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    AdminDescribeTransactionsEffect, AdminDescribeTransactionsInput,
    AdminDescribeTransactionsMachine, AdminDescribeTransactionsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeTransactionsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeTransactionsCall, DescribeTransactionsRawTerminal},
};

use super::{AdminDescribeTransactionsHostError, AdminDescribeTransactionsObserver};

pub(crate) const ADMIN_DESCRIBE_TRANSACTIONS_CAPACITY: usize = 16;
pub(crate) const ADMIN_DESCRIBE_TRANSACTIONS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AdminDescribeTransactionsAdmission {
    pub(crate) observer: AdminDescribeTransactionsObserver,
    pub(crate) fault: Option<AdminDescribeTransactionsHostError>,
}

/// One exact transactional ID ready for the engine's driver-admission stage.
pub(crate) struct AdminDescribeTransactionsSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    transactional_id: String,
}

impl AdminDescribeTransactionsSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, String) {
        (self.operation_id, self.deadline, self.transactional_id)
    }
}

pub(crate) enum AdminDescribeTransactionsTurn {
    Idle,
    Progress,
    Submit(AdminDescribeTransactionsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdminDescribeTransactionsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct AdminDescribeTransactionsOperation {
    operation_id: OperationId,
    machine: AdminDescribeTransactionsMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<AdminDescribeTransactionsSubmission>,
    handoff: AdminDescribeTransactionsHandoff,
    call: Option<DescribeTransactionsCall>,
    raw_terminal: Option<DescribeTransactionsRawTerminal>,
    terminal: Option<AdminDescribeTransactionsTerminal>,
}

pub(crate) struct AdminDescribeTransactionsHost {
    operations: Vec<AdminDescribeTransactionsOperation>,
    completions:
        CompletionRegistry<AdminDescribeTransactionsTerminal, AdminDescribeTransactionsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AdminDescribeTransactionsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AdminDescribeTransactionsHost {
    pub(crate) fn new(publisher: AdminDescribeTransactionsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ADMIN_DESCRIBE_TRANSACTIONS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ADMIN_DESCRIBE_TRANSACTIONS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ADMIN_DESCRIBE_TRANSACTIONS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AdminDescribeTransactionsTurn, AdminDescribeTransactionsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AdminDescribeTransactionsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AdminDescribeTransactionsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(
                operation_id,
                AdminDescribeTransactionsInput::DeadlineElapsed,
            )?;
            return Ok(AdminDescribeTransactionsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AdminDescribeTransactionsHostError::MissingSubmission)?;
        self.operations[index].handoff = AdminDescribeTransactionsHandoff::HandedOff;
        Ok(AdminDescribeTransactionsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeTransactionsCall,
    ) -> Result<(), AdminDescribeTransactionsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminDescribeTransactionsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminDescribeTransactionsHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(AdminDescribeTransactionsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, AdminDescribeTransactionsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), AdminDescribeTransactionsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminDescribeTransactionsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminDescribeTransactionsHandoff::HandedOff {
            return Err(AdminDescribeTransactionsHostError::InvalidHandoff);
        }
        self.apply(operation_id, AdminDescribeTransactionsInput::DriverRejected)
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
        input: AdminDescribeTransactionsInput,
    ) -> Result<(), AdminDescribeTransactionsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminDescribeTransactionsHostError::UnknownOperation)?;
        let accepted = matches!(&input, AdminDescribeTransactionsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AdminDescribeTransactionsHandoff::HandedOff
        {
            return Err(AdminDescribeTransactionsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AdminDescribeTransactionsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: AdminDescribeTransactionsEffect,
    ) -> Result<(), AdminDescribeTransactionsHostError> {
        match effect {
            AdminDescribeTransactionsEffect::Submit {
                operation_id,
                deadline,
                transactional_id,
            } if operation_id == self.operations[index].operation_id
                && deadline == self.operations[index].deadline.core() =>
            {
                self.operations[index].submission = Some(AdminDescribeTransactionsSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    transactional_id,
                });
                self.operations[index].handoff = AdminDescribeTransactionsHandoff::Untouched;
                Ok(())
            }
            AdminDescribeTransactionsEffect::Complete {
                operation_id,
                terminal,
            } if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(AdminDescribeTransactionsHostError::SubmissionMismatch),
        }
    }
}
