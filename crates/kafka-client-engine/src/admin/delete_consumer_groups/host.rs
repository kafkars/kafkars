//! Bounded ownership of Admin `DeleteConsumerGroups` machines and coordinator calls.

mod admission;
mod model;
mod recovered;
mod response;
mod terminal;

use kafka_client_core::{
    DeleteConsumerGroupsEffect, DeleteConsumerGroupsInput, DeleteConsumerGroupsPlan,
    DeleteConsumerGroupsTarget, DeleteConsumerGroupsTerminal, Moment, OperationId,
};

use crate::{
    admin::DeleteConsumerGroupsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::DeleteConsumerGroupsCall,
};

use super::{DeleteConsumerGroupsHostError, DeleteConsumerGroupsObserver};
use model::{DeleteConsumerGroupsHandoff, DeleteConsumerGroupsOperation};
pub(crate) use model::{DeleteConsumerGroupsSubmission, DeleteConsumerGroupsTurn};

pub(crate) const DELETE_CONSUMER_GROUPS_CAPACITY: usize = 16;
pub(crate) const DELETE_CONSUMER_GROUPS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DeleteConsumerGroupsAdmission {
    pub(crate) observer: DeleteConsumerGroupsObserver,
    pub(crate) fault: Option<DeleteConsumerGroupsHostError>,
}

pub(crate) struct DeleteConsumerGroupsHost {
    operations: Vec<DeleteConsumerGroupsOperation>,
    completions: CompletionRegistry<DeleteConsumerGroupsTerminal, DeleteConsumerGroupsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DeleteConsumerGroupsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DeleteConsumerGroupsHost {
    pub(crate) fn new(publisher: DeleteConsumerGroupsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DELETE_CONSUMER_GROUPS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                DELETE_CONSUMER_GROUPS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DELETE_CONSUMER_GROUPS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DeleteConsumerGroupsTurn, DeleteConsumerGroupsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DeleteConsumerGroupsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DeleteConsumerGroupsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DeleteConsumerGroupsInput::DeadlineElapsed)?;
            return Ok(DeleteConsumerGroupsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DeleteConsumerGroupsHostError::MissingSubmission)?;
        self.operations[index].handoff = DeleteConsumerGroupsHandoff::HandedOff;
        Ok(DeleteConsumerGroupsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DeleteConsumerGroupsCall,
    ) -> Result<(), DeleteConsumerGroupsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteConsumerGroupsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DeleteConsumerGroupsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(DeleteConsumerGroupsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        if !self.operations[index].call_matches_expected() {
            return Err(DeleteConsumerGroupsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, DeleteConsumerGroupsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: DeleteConsumerGroupsPlan,
        target: DeleteConsumerGroupsTarget,
        request_limit: usize,
        result_limit: usize,
    ) -> Result<(), DeleteConsumerGroupsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteConsumerGroupsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DeleteConsumerGroupsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(DeleteConsumerGroupsHostError::InvalidHandoff);
        }
        let matches =
            self.operations[index].matches_evidence(&plan, &target, request_limit, result_limit);
        self.operations[index].rejected_submission =
            Some((plan, target, request_limit, result_limit));
        if !matches {
            return Err(DeleteConsumerGroupsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index]
            .machine
            .apply(DeleteConsumerGroupsInput::DriverRejected)?;
        let effect = transition
            .into_effect()
            .ok_or(DeleteConsumerGroupsHostError::MissingTerminal)?;
        drop(self.operations[index].rejected_submission.take());
        self.install_effect(index, effect)
    }

    fn apply(
        &mut self,
        operation_id: OperationId,
        input: DeleteConsumerGroupsInput,
    ) -> Result<(), DeleteConsumerGroupsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteConsumerGroupsHostError::UnknownOperation)?;
        let accepted = matches!(&input, DeleteConsumerGroupsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DeleteConsumerGroupsHandoff::HandedOff {
            return Err(DeleteConsumerGroupsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DeleteConsumerGroupsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: DeleteConsumerGroupsEffect,
    ) -> Result<(), DeleteConsumerGroupsHostError> {
        let operation_id = self.operations[index].operation_id;
        match effect {
            DeleteConsumerGroupsEffect::Submit {
                operation_id: effect_id,
                deadline,
                target,
            } => {
                if effect_id != operation_id
                    || deadline != self.operations[index].deadline.core()
                    || self.operations[index].machine.current_target() != Some(&target)
                    || !self.operations[index].plan.targets().contains(&target)
                {
                    return Err(DeleteConsumerGroupsHostError::SubmissionMismatch);
                }
                self.operations[index].result_limit = self.operations[index].remaining_result_bytes;
                self.operations[index].submission = Some(DeleteConsumerGroupsSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    plan: self.operations[index].plan.clone(),
                    target,
                    request_limit: self.operations[index].request_limit,
                    result_limit: self.operations[index].result_limit,
                });
                self.operations[index].handoff = DeleteConsumerGroupsHandoff::Untouched;
                Ok(())
            }
            DeleteConsumerGroupsEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(DeleteConsumerGroupsHostError::SubmissionMismatch);
                }
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
        }
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
