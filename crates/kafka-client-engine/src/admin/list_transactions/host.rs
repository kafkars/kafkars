//! Bounded ownership of cluster transaction-listing machines and concrete calls.

mod admission;
mod effect;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    AdminListTransactionsInput, AdminListTransactionsMachine, AdminListTransactionsTerminal,
    Moment, OperationId,
};

use crate::{
    admin::AdminListTransactionsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{ListTransactionsCall, ListTransactionsRawTerminal},
};

use super::{AdminListTransactionsHostError, AdminListTransactionsObserver};

use model::{AdminListTransactionsHandoff, AdminListTransactionsSubmission};
pub(crate) use model::{AdminListTransactionsSubmissionKind, AdminListTransactionsTurn};

pub(crate) const ADMIN_LIST_TRANSACTIONS_CAPACITY: usize = 16;
pub(super) const ADMIN_LIST_TRANSACTIONS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AdminListTransactionsAdmission {
    pub(crate) observer: AdminListTransactionsObserver,
    pub(crate) fault: Option<AdminListTransactionsHostError>,
}

struct AdminListTransactionsOperation {
    operation_id: OperationId,
    machine: AdminListTransactionsMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<AdminListTransactionsSubmission>,
    active_submission: Option<AdminListTransactionsSubmissionKind>,
    handoff: AdminListTransactionsHandoff,
    call: Option<ListTransactionsCall>,
    recovered_call: Option<ListTransactionsCall>,
    raw_terminal: Option<ListTransactionsRawTerminal>,
    terminal: Option<AdminListTransactionsTerminal>,
}

pub(crate) struct AdminListTransactionsHost {
    operations: Vec<AdminListTransactionsOperation>,
    completions: CompletionRegistry<AdminListTransactionsTerminal, AdminListTransactionsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AdminListTransactionsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AdminListTransactionsHost {
    pub(crate) fn new(publisher: AdminListTransactionsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ADMIN_LIST_TRANSACTIONS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ADMIN_LIST_TRANSACTIONS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ADMIN_LIST_TRANSACTIONS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AdminListTransactionsTurn, AdminListTransactionsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AdminListTransactionsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AdminListTransactionsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, AdminListTransactionsInput::DeadlineElapsed)?;
            return Ok(AdminListTransactionsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AdminListTransactionsHostError::MissingSubmission)?;
        self.operations[index].handoff = AdminListTransactionsHandoff::HandedOff;
        Ok(AdminListTransactionsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: ListTransactionsCall,
    ) -> Result<(), AdminListTransactionsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminListTransactionsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminListTransactionsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(AdminListTransactionsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        if !call_matches_active_submission(&self.operations[index]) {
            return Err(AdminListTransactionsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, AdminListTransactionsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        submission: AdminListTransactionsSubmissionKind,
    ) -> Result<(), AdminListTransactionsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminListTransactionsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AdminListTransactionsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(AdminListTransactionsHostError::InvalidHandoff);
        }
        if self.operations[index].active_submission.as_ref() != Some(&submission) {
            return Err(AdminListTransactionsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, AdminListTransactionsInput::DriverRejected)
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
        input: AdminListTransactionsInput,
    ) -> Result<(), AdminListTransactionsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AdminListTransactionsHostError::UnknownOperation)?;
        let accepted = matches!(&input, AdminListTransactionsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AdminListTransactionsHandoff::HandedOff {
            return Err(AdminListTransactionsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AdminListTransactionsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::admin::list_transactions) fn apply_input_for_test(
        &mut self,
        operation_id: OperationId,
        input: AdminListTransactionsInput,
    ) -> Result<(), AdminListTransactionsHostError> {
        self.apply(operation_id, input)
    }
}

fn call_matches_active_submission(operation: &AdminListTransactionsOperation) -> bool {
    let (Some(submission), Some(call)) = (
        operation.active_submission.as_ref(),
        operation.call.as_ref(),
    ) else {
        return false;
    };
    match submission {
        AdminListTransactionsSubmissionKind::Discovery { retained_limit } => {
            call.matches_discovery(*retained_limit)
        }
        AdminListTransactionsSubmissionKind::Broker {
            broker_id,
            plan,
            retained_limit,
        } => call.matches_broker(*broker_id, plan, *retained_limit),
    }
}
