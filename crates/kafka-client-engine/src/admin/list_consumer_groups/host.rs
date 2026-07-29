//! Bounded ownership of cluster group-listing machines and concrete calls.

mod admission;
mod effect;
mod model;
#[cfg(test)]
mod ownership_test;
mod response;
mod terminal;

use kafka_client_core::{
    AdminListConsumerGroupsInput, AdminListConsumerGroupsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminListConsumerGroupsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::ListConsumerGroupsCall,
};

use super::{ListConsumerGroupsHostError, ListConsumerGroupsObserver};

use model::{ListConsumerGroupsHandoff, ListConsumerGroupsOperation};
pub(crate) use model::{
    ListConsumerGroupsSubmission, ListConsumerGroupsSubmissionKind, ListConsumerGroupsTurn,
};

pub(crate) const LIST_CONSUMER_GROUPS_CAPACITY: usize = 16;
const LIST_CONSUMER_GROUPS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct ListConsumerGroupsAdmission {
    pub(crate) observer: ListConsumerGroupsObserver,
    pub(crate) fault: Option<ListConsumerGroupsHostError>,
}

pub(crate) struct ListConsumerGroupsHost {
    operations: Vec<ListConsumerGroupsOperation>,
    completions:
        CompletionRegistry<AdminListConsumerGroupsTerminal, AdminListConsumerGroupsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<ListConsumerGroupsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl ListConsumerGroupsHost {
    pub(crate) fn new(publisher: AdminListConsumerGroupsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(LIST_CONSUMER_GROUPS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                LIST_CONSUMER_GROUPS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(LIST_CONSUMER_GROUPS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<ListConsumerGroupsTurn, ListConsumerGroupsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(ListConsumerGroupsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(ListConsumerGroupsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, AdminListConsumerGroupsInput::DeadlineElapsed)?;
            return Ok(ListConsumerGroupsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(ListConsumerGroupsHostError::MissingSubmission)?;
        self.operations[index].handoff = ListConsumerGroupsHandoff::HandedOff;
        Ok(ListConsumerGroupsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: ListConsumerGroupsCall,
    ) -> Result<(), ListConsumerGroupsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConsumerGroupsHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListConsumerGroupsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(ListConsumerGroupsHostError::InvalidHandoff);
        }
        let matches = self.operations[index].matches_call(&call);
        self.operations[index].call = Some(call);
        if !matches {
            return Err(ListConsumerGroupsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, AdminListConsumerGroupsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        kind: ListConsumerGroupsSubmissionKind,
    ) -> Result<(), ListConsumerGroupsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConsumerGroupsHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListConsumerGroupsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(ListConsumerGroupsHostError::InvalidHandoff);
        }
        let matches = self.operations[index].matches_submission(&kind);
        self.operations[index].rejected_submission = Some(kind);
        if !matches {
            return Err(ListConsumerGroupsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index]
            .machine
            .apply(AdminListConsumerGroupsInput::DriverRejected)?;
        drop(self.operations[index].rejected_submission.take());
        let effect = transition
            .into_effect()
            .ok_or(ListConsumerGroupsHostError::MissingTerminal)?;
        self.install_effect(index, effect)
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
        input: AdminListConsumerGroupsInput,
    ) -> Result<(), ListConsumerGroupsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConsumerGroupsHostError::UnknownOperation)?;
        let accepted = matches!(&input, AdminListConsumerGroupsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != ListConsumerGroupsHandoff::HandedOff {
            return Err(ListConsumerGroupsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = ListConsumerGroupsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }
}
