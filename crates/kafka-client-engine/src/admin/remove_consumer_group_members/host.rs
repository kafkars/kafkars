//! Bounded ownership of accepted member-removal machines and tracked calls.

mod admission;
mod model;
#[cfg(test)]
mod ownership_test;
mod recovered;
mod response;
mod terminal;

use kafka_client_core::{
    Moment, OperationId, RemoveConsumerGroupMembersEffect, RemoveConsumerGroupMembersInput,
    RemoveConsumerGroupMembersPlan, RemoveConsumerGroupMembersTerminal as CoreTerminal,
};

use crate::{
    admin::RemoveConsumerGroupMembersPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::RemoveConsumerGroupMembersCall,
};

use super::{RemoveConsumerGroupMembersHostError, RemoveConsumerGroupMembersObserver};
use model::{RemoveConsumerGroupMembersHandoff, RemoveConsumerGroupMembersOperation};
pub(crate) use model::{RemoveConsumerGroupMembersSubmission, RemoveConsumerGroupMembersTurn};

pub(crate) const REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY: usize = 16;
pub(crate) const REMOVE_CONSUMER_GROUP_MEMBERS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct RemoveConsumerGroupMembersAdmission {
    pub(crate) observer: RemoveConsumerGroupMembersObserver,
    pub(crate) fault: Option<RemoveConsumerGroupMembersHostError>,
}

pub(crate) struct RemoveConsumerGroupMembersHost {
    operations: Vec<RemoveConsumerGroupMembersOperation>,
    completions: CompletionRegistry<CoreTerminal, RemoveConsumerGroupMembersPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<RemoveConsumerGroupMembersHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl RemoveConsumerGroupMembersHost {
    pub(crate) fn new(publisher: RemoveConsumerGroupMembersPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<RemoveConsumerGroupMembersTurn, RemoveConsumerGroupMembersHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(RemoveConsumerGroupMembersTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(RemoveConsumerGroupMembersTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(
                operation_id,
                RemoveConsumerGroupMembersInput::DeadlineElapsed,
            )?;
            return Ok(RemoveConsumerGroupMembersTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(RemoveConsumerGroupMembersHostError::MissingSubmission)?;
        self.operations[index].handoff = RemoveConsumerGroupMembersHandoff::HandedOff;
        Ok(RemoveConsumerGroupMembersTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: RemoveConsumerGroupMembersCall,
    ) -> Result<(), RemoveConsumerGroupMembersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(RemoveConsumerGroupMembersHostError::UnknownOperation)?;
        if self.operations[index].handoff != RemoveConsumerGroupMembersHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(RemoveConsumerGroupMembersHostError::InvalidHandoff);
        }
        let matches = self.operations[index].matches_call(&call);
        self.operations[index].call = Some(call);
        if !matches {
            return Err(RemoveConsumerGroupMembersHostError::SubmissionMismatch);
        }
        self.apply(
            operation_id,
            RemoveConsumerGroupMembersInput::DriverAccepted,
        )
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: RemoveConsumerGroupMembersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Result<(), RemoveConsumerGroupMembersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(RemoveConsumerGroupMembersHostError::UnknownOperation)?;
        if self.operations[index].handoff != RemoveConsumerGroupMembersHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(RemoveConsumerGroupMembersHostError::InvalidHandoff);
        }
        let matches =
            self.operations[index].matches_submission(&plan, request_scratch_limit, result_limit);
        self.operations[index].rejected_submission =
            Some((plan, request_scratch_limit, result_limit));
        if !matches {
            return Err(RemoveConsumerGroupMembersHostError::SubmissionMismatch);
        }
        let transition = self.operations[index]
            .machine
            .apply(RemoveConsumerGroupMembersInput::DriverRejected)?;
        let terminal = match transition.into_effect() {
            Some(RemoveConsumerGroupMembersEffect::Complete {
                operation_id: effect_id,
                terminal,
            }) if effect_id == operation_id => terminal,
            _ => return Err(RemoveConsumerGroupMembersHostError::MissingTerminal),
        };
        drop(self.operations[index].rejected_submission.take());
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
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
        input: RemoveConsumerGroupMembersInput,
    ) -> Result<(), RemoveConsumerGroupMembersHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(RemoveConsumerGroupMembersHostError::UnknownOperation)?;
        let accepted = matches!(&input, RemoveConsumerGroupMembersInput::DriverAccepted);
        if accepted
            && self.operations[index].handoff != RemoveConsumerGroupMembersHandoff::HandedOff
        {
            return Err(RemoveConsumerGroupMembersHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = RemoveConsumerGroupMembersHandoff::Submitted;
        }
        if let Some(RemoveConsumerGroupMembersEffect::Complete {
            operation_id: effect_id,
            terminal,
        }) = transition.into_effect()
        {
            if effect_id != operation_id {
                return Err(RemoveConsumerGroupMembersHostError::SubmissionMismatch);
            }
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }
}
