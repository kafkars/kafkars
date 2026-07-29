//! Bounded ownership of share-group description machines and concrete calls.

mod admission;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DescribeShareGroupEffect, DescribeShareGroupInput, DescribeShareGroupMachine,
    DescribeShareGroupPlan, DescribeShareGroupTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeShareGroupPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeShareGroupCall, DescribeShareGroupTerminal as DriverTerminal},
};

use super::{DescribeShareGroupHostError, DescribeShareGroupObserver};

use model::DescribeShareGroupHandoff;
pub(crate) use model::{DescribeShareGroupSubmission, DescribeShareGroupTurn};

pub(crate) const DESCRIBE_SHARE_GROUP_CAPACITY: usize = 16;
pub(crate) const DESCRIBE_SHARE_GROUP_RESULT_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const DESCRIBE_SHARE_GROUP_RETAINED_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct DescribeShareGroupAdmission {
    pub(crate) observer: DescribeShareGroupObserver,
    pub(crate) fault: Option<DescribeShareGroupHostError>,
}

struct DescribeShareGroupOperation {
    operation_id: OperationId,
    machine: DescribeShareGroupMachine,
    active_plan: Option<DescribeShareGroupPlan>,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<DescribeShareGroupSubmission>,
    handoff: DescribeShareGroupHandoff,
    call: Option<DescribeShareGroupCall>,
    raw_terminal: Option<DriverTerminal>,
    terminal: Option<DescribeShareGroupTerminal>,
}

impl DescribeShareGroupOperation {
    fn active_plan(&self) -> Result<&DescribeShareGroupPlan, DescribeShareGroupHostError> {
        self.active_plan
            .as_ref()
            .ok_or(DescribeShareGroupHostError::MissingSubmission)
    }

    fn install_submission(
        &mut self,
        effect_id: OperationId,
        core_deadline: kafka_client_core::Deadline,
        plan: DescribeShareGroupPlan,
    ) -> Result<(), DescribeShareGroupHostError> {
        if effect_id != self.operation_id
            || core_deadline != self.deadline.core()
            || plan.group_ids().len() != 1
        {
            return Err(DescribeShareGroupHostError::SubmissionMismatch);
        }
        if self.call.is_some() || self.raw_terminal.is_some() || self.terminal.is_some() {
            return Err(DescribeShareGroupHostError::InvalidHandoff);
        }
        self.active_plan = Some(plan.clone());
        self.submission = Some(DescribeShareGroupSubmission {
            operation_id: self.operation_id,
            deadline: self.deadline,
            plan,
            result_limit: self.remaining_result_bytes,
        });
        self.handoff = DescribeShareGroupHandoff::Untouched;
        Ok(())
    }

    fn install_terminal(
        &mut self,
        effect_id: OperationId,
        terminal: DescribeShareGroupTerminal,
    ) -> Result<(), DescribeShareGroupHostError> {
        if effect_id != self.operation_id {
            return Err(DescribeShareGroupHostError::SubmissionMismatch);
        }
        if self.call.is_some() || self.raw_terminal.is_some() || self.terminal.is_some() {
            return Err(DescribeShareGroupHostError::InvalidHandoff);
        }
        self.active_plan = None;
        self.submission = None;
        self.terminal = Some(terminal);
        Ok(())
    }

    fn debit_result_bytes(
        &mut self,
        retained_bytes: usize,
    ) -> Result<(), DescribeShareGroupHostError> {
        self.remaining_result_bytes = self
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(DescribeShareGroupHostError::ByteAccounting)?;
        Ok(())
    }
}

pub(crate) struct DescribeShareGroupHost {
    operations: Vec<DescribeShareGroupOperation>,
    completions: CompletionRegistry<DescribeShareGroupTerminal, AdminDescribeShareGroupPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeShareGroupHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeShareGroupHost {
    pub(crate) fn new(publisher: AdminDescribeShareGroupPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_SHARE_GROUP_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                DESCRIBE_SHARE_GROUP_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_SHARE_GROUP_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeShareGroupTurn, DescribeShareGroupHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeShareGroupTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeShareGroupTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DescribeShareGroupInput::DeadlineElapsed)?;
            return Ok(DescribeShareGroupTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeShareGroupHostError::MissingSubmission)?;
        self.operations[index].handoff = DescribeShareGroupHandoff::HandedOff;
        Ok(DescribeShareGroupTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeShareGroupCall,
    ) -> Result<(), DescribeShareGroupHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeShareGroupHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeShareGroupHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(DescribeShareGroupHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, DescribeShareGroupInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), DescribeShareGroupHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeShareGroupHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeShareGroupHandoff::HandedOff {
            return Err(DescribeShareGroupHostError::InvalidHandoff);
        }
        self.apply(operation_id, DescribeShareGroupInput::DriverRejected)
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
        input: DescribeShareGroupInput,
    ) -> Result<(), DescribeShareGroupHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeShareGroupHostError::UnknownOperation)?;
        let accepted = matches!(&input, DescribeShareGroupInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DescribeShareGroupHandoff::HandedOff {
            return Err(DescribeShareGroupHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DescribeShareGroupHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: DescribeShareGroupEffect,
    ) -> Result<(), DescribeShareGroupHostError> {
        match effect {
            DescribeShareGroupEffect::Submit {
                operation_id,
                deadline,
                plan,
            } => self.operations[index].install_submission(operation_id, deadline, plan),
            DescribeShareGroupEffect::Complete {
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
        input: DescribeShareGroupInput,
        retained_bytes: usize,
    ) -> Result<(), DescribeShareGroupHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeShareGroupHostError::UnknownOperation)?;
        self.operations[index].debit_result_bytes(retained_bytes)?;
        self.apply(operation_id, input)
    }
}
