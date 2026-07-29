//! Bounded ownership of accepted Admin `FenceProducers` machines and coordinator calls.

mod admission;
mod response;
mod terminal;

use kafka_client_core::{
    AdminFenceProducersEffect, AdminFenceProducersInput, AdminFenceProducersMachine,
    AdminFenceProducersTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminFenceProducersPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{TransactionInitCall, TransactionInitTerminal},
};

use super::{AdminFenceProducersHostError, AdminFenceProducersObserver};

pub(crate) const ADMIN_FENCE_PRODUCERS_CAPACITY: usize = 16;
pub(crate) const ADMIN_FENCE_PRODUCERS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AdminFenceProducersAdmission {
    pub(crate) observer: AdminFenceProducersObserver,
    pub(crate) fault: Option<AdminFenceProducersHostError>,
}

/// One exact transactional ID ready for the engine's driver-admission stage.
pub(crate) struct AdminFenceProducersSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    transactional_id: String,
}

impl AdminFenceProducersSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, String) {
        (self.operation_id, self.deadline, self.transactional_id)
    }
}

pub(crate) enum AdminFenceProducersTurn {
    Idle,
    Progress,
    Submit(AdminFenceProducersSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdminFenceProducersHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct AdminFenceProducersOperation {
    operation_id: OperationId,
    machine: AdminFenceProducersMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<AdminFenceProducersSubmission>,
    handoff: AdminFenceProducersHandoff,
    call: Option<TransactionInitCall>,
    raw_terminal: Option<TransactionInitTerminal>,
    terminal: Option<AdminFenceProducersTerminal>,
}

pub(crate) struct AdminFenceProducersHost {
    operations: Vec<AdminFenceProducersOperation>,
    completions: CompletionRegistry<AdminFenceProducersTerminal, AdminFenceProducersPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AdminFenceProducersHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AdminFenceProducersHost {
    pub(crate) fn new(publisher: AdminFenceProducersPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ADMIN_FENCE_PRODUCERS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ADMIN_FENCE_PRODUCERS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ADMIN_FENCE_PRODUCERS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AdminFenceProducersTurn, AdminFenceProducersHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AdminFenceProducersTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AdminFenceProducersTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, AdminFenceProducersInput::DeadlineElapsed)?;
            return Ok(AdminFenceProducersTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AdminFenceProducersHostError::MissingSubmission)?;
        self.operations[index].handoff = AdminFenceProducersHandoff::HandedOff;
        Ok(AdminFenceProducersTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: TransactionInitCall,
    ) -> Result<(), AdminFenceProducersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminFenceProducersHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminFenceProducersHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(AdminFenceProducersHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, AdminFenceProducersInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), AdminFenceProducersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminFenceProducersHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminFenceProducersHandoff::HandedOff {
            return Err(AdminFenceProducersHostError::InvalidHandoff);
        }
        self.apply(operation_id, AdminFenceProducersInput::DriverRejected)
    }

    pub(crate) fn expire_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), AdminFenceProducersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminFenceProducersHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminFenceProducersHandoff::HandedOff {
            return Err(AdminFenceProducersHostError::InvalidHandoff);
        }
        self.apply(operation_id, AdminFenceProducersInput::DeadlineElapsed)
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
        input: AdminFenceProducersInput,
    ) -> Result<(), AdminFenceProducersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminFenceProducersHostError::UnknownOperation)?;
        let accepted = matches!(&input, AdminFenceProducersInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AdminFenceProducersHandoff::HandedOff {
            return Err(AdminFenceProducersHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AdminFenceProducersHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: AdminFenceProducersEffect,
    ) -> Result<(), AdminFenceProducersHostError> {
        match effect {
            AdminFenceProducersEffect::Submit {
                operation_id,
                deadline,
                transactional_id,
            } if operation_id == self.operations[index].operation_id
                && deadline == self.operations[index].deadline.core() =>
            {
                self.operations[index].submission = Some(AdminFenceProducersSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    transactional_id,
                });
                self.operations[index].handoff = AdminFenceProducersHandoff::Untouched;
                Ok(())
            }
            AdminFenceProducersEffect::Complete {
                operation_id,
                terminal,
            } if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(AdminFenceProducersHostError::SubmissionMismatch),
        }
    }
}
