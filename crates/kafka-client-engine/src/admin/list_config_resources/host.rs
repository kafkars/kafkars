//! Bounded ownership of configuration-resource machines and concrete calls.

mod admission;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    ListConfigResourcesEffect, ListConfigResourcesInput, ListConfigResourcesMachine,
    ListConfigResourcesTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminListConfigResourcesPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{
        ListConfigResourcesCall, ListConfigResourcesRawTerminal, RecoveredListConfigResourcesCall,
    },
};

use super::{ListConfigResourcesHostError, ListConfigResourcesObserver};

use model::ListConfigResourcesHandoff;
pub(crate) use model::{ListConfigResourcesSubmission, ListConfigResourcesTurn};

pub(crate) const LIST_CONFIG_RESOURCES_CAPACITY: usize = 16;
pub(crate) const LIST_CONFIG_RESOURCES_RESULT_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const LIST_CONFIG_RESOURCES_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct ListConfigResourcesAdmission {
    pub(crate) observer: ListConfigResourcesObserver,
    pub(crate) fault: Option<ListConfigResourcesHostError>,
}

struct ListConfigResourcesOperation {
    operation_id: OperationId,
    machine: ListConfigResourcesMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<ListConfigResourcesSubmission>,
    handoff: ListConfigResourcesHandoff,
    call: Option<ListConfigResourcesCall>,
    recovered_call: Option<RecoveredListConfigResourcesCall>,
    raw_terminal: Option<ListConfigResourcesRawTerminal>,
    terminal: Option<ListConfigResourcesTerminal>,
}

pub(crate) struct ListConfigResourcesHost {
    operations: Vec<ListConfigResourcesOperation>,
    completions: CompletionRegistry<ListConfigResourcesTerminal, AdminListConfigResourcesPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<ListConfigResourcesHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl ListConfigResourcesHost {
    pub(crate) fn new(publisher: AdminListConfigResourcesPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(LIST_CONFIG_RESOURCES_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                LIST_CONFIG_RESOURCES_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(LIST_CONFIG_RESOURCES_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<ListConfigResourcesTurn, ListConfigResourcesHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(ListConfigResourcesTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(ListConfigResourcesTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, ListConfigResourcesInput::DeadlineElapsed)?;
            return Ok(ListConfigResourcesTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(ListConfigResourcesHostError::MissingSubmission)?;
        self.operations[index].handoff = ListConfigResourcesHandoff::HandedOff;
        Ok(ListConfigResourcesTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: ListConfigResourcesCall,
    ) -> Result<(), ListConfigResourcesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConfigResourcesHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListConfigResourcesHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
        {
            return Err(ListConfigResourcesHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, ListConfigResourcesInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), ListConfigResourcesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConfigResourcesHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListConfigResourcesHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(ListConfigResourcesHostError::InvalidHandoff);
        }
        self.apply(operation_id, ListConfigResourcesInput::DriverRejected)
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
        input: ListConfigResourcesInput,
    ) -> Result<(), ListConfigResourcesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConfigResourcesHostError::UnknownOperation)?;
        let accepted = matches!(&input, ListConfigResourcesInput::DriverAccepted);
        if accepted && self.operations[index].handoff != ListConfigResourcesHandoff::HandedOff {
            return Err(ListConfigResourcesHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = ListConfigResourcesHandoff::Submitted;
        }
        if let Some(ListConfigResourcesEffect::Complete { terminal, .. }) = transition.into_effect()
        {
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
