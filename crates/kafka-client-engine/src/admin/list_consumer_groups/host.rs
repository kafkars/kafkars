//! Bounded ownership of cluster group-listing machines and concrete calls.

mod admission;
mod model;
mod response;
mod terminal;

use kafka_client_core::{
    AdminListConsumerGroupsEffect, AdminListConsumerGroupsInput, AdminListConsumerGroupsMachine,
    AdminListConsumerGroupsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminListConsumerGroupsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{ListConsumerGroupsCall, ListConsumerGroupsRawTerminal},
};

use super::{ListConsumerGroupsHostError, ListConsumerGroupsObserver};

use model::ListConsumerGroupsHandoff;
pub(crate) use model::{
    ListConsumerGroupsSubmission, ListConsumerGroupsSubmissionKind, ListConsumerGroupsTurn,
};

pub(crate) const LIST_CONSUMER_GROUPS_CAPACITY: usize = 16;
const LIST_CONSUMER_GROUPS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct ListConsumerGroupsAdmission {
    pub(crate) observer: ListConsumerGroupsObserver,
    pub(crate) fault: Option<ListConsumerGroupsHostError>,
}

struct ListConsumerGroupsOperation {
    operation_id: OperationId,
    machine: AdminListConsumerGroupsMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<ListConsumerGroupsSubmission>,
    handoff: ListConsumerGroupsHandoff,
    call: Option<ListConsumerGroupsCall>,
    raw_terminal: Option<ListConsumerGroupsRawTerminal>,
    terminal: Option<AdminListConsumerGroupsTerminal>,
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
        {
            return Err(ListConsumerGroupsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, AdminListConsumerGroupsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), ListConsumerGroupsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConsumerGroupsHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListConsumerGroupsHandoff::HandedOff {
            return Err(ListConsumerGroupsHostError::InvalidHandoff);
        }
        self.apply(operation_id, AdminListConsumerGroupsInput::DriverRejected)
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
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = ListConsumerGroupsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: AdminListConsumerGroupsEffect,
    ) -> Result<(), ListConsumerGroupsHostError> {
        let operation_id = self.operations[index].operation_id;
        let (effect_id, kind) = match effect {
            AdminListConsumerGroupsEffect::SubmitDiscovery {
                operation_id,
                deadline,
            } => {
                if deadline != self.operations[index].deadline.core() {
                    return Err(ListConsumerGroupsHostError::SubmissionMismatch);
                }
                (operation_id, ListConsumerGroupsSubmissionKind::Discovery)
            }
            AdminListConsumerGroupsEffect::SubmitBroker {
                operation_id,
                deadline,
                broker_id,
            } => {
                if deadline != self.operations[index].deadline.core() {
                    return Err(ListConsumerGroupsHostError::SubmissionMismatch);
                }
                (
                    operation_id,
                    ListConsumerGroupsSubmissionKind::Broker { broker_id },
                )
            }
            AdminListConsumerGroupsEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(ListConsumerGroupsHostError::SubmissionMismatch);
                }
                self.operations[index].terminal = Some(terminal);
                return self.publish_terminal(index);
            }
        };
        if effect_id != operation_id {
            return Err(ListConsumerGroupsHostError::SubmissionMismatch);
        }
        self.operations[index].submission = Some(ListConsumerGroupsSubmission {
            operation_id,
            deadline: self.operations[index].deadline,
            kind,
        });
        self.operations[index].handoff = ListConsumerGroupsHandoff::Untouched;
        Ok(())
    }
}
