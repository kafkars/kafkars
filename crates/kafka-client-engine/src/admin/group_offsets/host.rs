//! Bounded ownership of singular and batched group-offset machines and calls.

mod admission;
mod operation;
mod response;
mod state;
mod submission;
mod terminal;

#[cfg(test)]
mod ownership_test;

use kafka_client_core::{
    ListConsumerGroupOffsetsEffect, ListConsumerGroupOffsetsInput, ListConsumerGroupOffsetsMachine,
    ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsTerminal, Moment, OperationId,
};

use crate::{
    admin::ListConsumerGroupOffsetsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{GroupOffsetsCall, GroupOffsetsTerminal, RecoveredGroupOffsetsCall},
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
    active_plan: Option<ListConsumerGroupOffsetsPlan>,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<ListConsumerGroupOffsetsSubmission>,
    handoff: ListConsumerGroupOffsetsHandoff,
    call: Option<GroupOffsetsCall>,
    recovered_call: Option<RecoveredGroupOffsetsCall>,
    raw_terminal: Option<GroupOffsetsTerminal>,
    rejected_submission: Option<(ListConsumerGroupOffsetsPlan, usize)>,
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
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: ListConsumerGroupOffsetsEffect,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        match effect {
            ListConsumerGroupOffsetsEffect::Submit {
                operation_id,
                deadline,
                plan,
            } => self.operations[index].install_submission(operation_id, deadline, plan),
            ListConsumerGroupOffsetsEffect::Complete {
                operation_id,
                terminal,
            } => {
                self.operations[index].install_terminal(operation_id, terminal)?;
                self.publish_terminal(index)
            }
        }
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }

    #[cfg(test)]
    pub(super) fn apply_for_test(
        &mut self,
        operation_id: OperationId,
        input: ListConsumerGroupOffsetsInput,
        retained_bytes: usize,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListConsumerGroupOffsetsHostError::UnknownOperation)?;
        self.operations[index].debit_result_bytes(retained_bytes)?;
        self.apply(operation_id, input)
    }
}

impl ListConsumerGroupOffsetsOperation {
    fn mark_untouched(&mut self) {
        self.handoff = ListConsumerGroupOffsetsHandoff::Untouched;
    }

    fn mark_handed_off(&mut self) {
        self.handoff = ListConsumerGroupOffsetsHandoff::HandedOff;
    }

    fn mark_submitted(&mut self) {
        self.handoff = ListConsumerGroupOffsetsHandoff::Submitted;
    }
}
