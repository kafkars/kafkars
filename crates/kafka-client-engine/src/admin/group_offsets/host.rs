//! Bounded ownership of accepted consumer-group offset machines and calls.

mod admission;
mod response;
mod terminal;

use kafka_client_core::{
    ListConsumerGroupOffsetsEffect, ListConsumerGroupOffsetsInput, ListConsumerGroupOffsetsMachine,
    ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsTerminal, Moment, OperationId,
};

use crate::{
    admin::ListConsumerGroupOffsetsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{GroupOffsetsCall, GroupOffsetsTerminal},
};

use super::{ListConsumerGroupOffsetsHostError, ListConsumerGroupOffsetsObserver};

pub(crate) const LIST_CONSUMER_GROUP_OFFSETS_CAPACITY: usize = 16;
pub(crate) const LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct ListConsumerGroupOffsetsAdmission {
    pub(crate) observer: ListConsumerGroupOffsetsObserver,
    pub(crate) fault: Option<ListConsumerGroupOffsetsHostError>,
}

pub(crate) struct ListConsumerGroupOffsetsSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: ListConsumerGroupOffsetsPlan,
    result_limit: usize,
}

impl ListConsumerGroupOffsetsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        ListConsumerGroupOffsetsPlan,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum ListConsumerGroupOffsetsTurn {
    Idle,
    Progress,
    Submit(ListConsumerGroupOffsetsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListConsumerGroupOffsetsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct ListConsumerGroupOffsetsOperation {
    operation_id: OperationId,
    machine: ListConsumerGroupOffsetsMachine,
    group_id: String,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    result_limit: usize,
    submission: Option<ListConsumerGroupOffsetsSubmission>,
    handoff: ListConsumerGroupOffsetsHandoff,
    call: Option<GroupOffsetsCall>,
    raw_terminal: Option<GroupOffsetsTerminal>,
    terminal: Option<ListConsumerGroupOffsetsTerminal>,
}

pub(crate) struct ListConsumerGroupOffsetsHost {
    operations: Vec<ListConsumerGroupOffsetsOperation>,
    completions:
        CompletionRegistry<ListConsumerGroupOffsetsTerminal, ListConsumerGroupOffsetsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<ListConsumerGroupOffsetsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl ListConsumerGroupOffsetsHost {
    pub(crate) fn new(publisher: ListConsumerGroupOffsetsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(LIST_CONSUMER_GROUP_OFFSETS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                LIST_CONSUMER_GROUP_OFFSETS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(LIST_CONSUMER_GROUP_OFFSETS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<ListConsumerGroupOffsetsTurn, ListConsumerGroupOffsetsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(ListConsumerGroupOffsetsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(ListConsumerGroupOffsetsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, ListConsumerGroupOffsetsInput::DeadlineElapsed)?;
            return Ok(ListConsumerGroupOffsetsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(ListConsumerGroupOffsetsHostError::MissingSubmission)?;
        self.operations[index].mark_handed_off();
        Ok(ListConsumerGroupOffsetsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: GroupOffsetsCall,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConsumerGroupOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListConsumerGroupOffsetsHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(ListConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, ListConsumerGroupOffsetsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConsumerGroupOffsetsHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListConsumerGroupOffsetsHandoff::HandedOff {
            return Err(ListConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        self.apply(operation_id, ListConsumerGroupOffsetsInput::DriverRejected)
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
        input: ListConsumerGroupOffsetsInput,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConsumerGroupOffsetsHostError::UnknownOperation)?;
        let accepted = matches!(&input, ListConsumerGroupOffsetsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != ListConsumerGroupOffsetsHandoff::HandedOff
        {
            return Err(ListConsumerGroupOffsetsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].mark_submitted();
        }
        if let Some(ListConsumerGroupOffsetsEffect::Complete { terminal, .. }) =
            transition.into_effect()
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

impl ListConsumerGroupOffsetsOperation {
    fn mark_handed_off(&mut self) {
        self.handoff = ListConsumerGroupOffsetsHandoff::HandedOff;
    }

    fn mark_submitted(&mut self) {
        self.handoff = ListConsumerGroupOffsetsHandoff::Submitted;
    }
}
