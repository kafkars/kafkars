//! Bounded ownership of streams-group description machines and concrete calls.

mod admission;
mod model;
mod recovery;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DescribeStreamsGroupEffect, DescribeStreamsGroupInput, DescribeStreamsGroupMachine,
    DescribeStreamsGroupPlan, DescribeStreamsGroupTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeStreamsGroupPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeStreamsGroupCall, DescribeStreamsGroupTerminal as DriverTerminal},
};

use super::{DescribeStreamsGroupHostError, DescribeStreamsGroupObserver};

use model::DescribeStreamsGroupHandoff;
pub(crate) use model::{DescribeStreamsGroupSubmission, DescribeStreamsGroupTurn};

pub(crate) const DESCRIBE_STREAMS_GROUP_CAPACITY: usize = 16;
pub(crate) const DESCRIBE_STREAMS_GROUP_RESULT_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const DESCRIBE_STREAMS_GROUP_RETAINED_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct DescribeStreamsGroupAdmission {
    pub(crate) observer: DescribeStreamsGroupObserver,
    pub(crate) fault: Option<DescribeStreamsGroupHostError>,
}

struct DescribeStreamsGroupOperation {
    operation_id: OperationId,
    machine: DescribeStreamsGroupMachine,
    active_plan: Option<DescribeStreamsGroupPlan>,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<DescribeStreamsGroupSubmission>,
    handoff: DescribeStreamsGroupHandoff,
    call: Option<DescribeStreamsGroupCall>,
    recovered_call: Option<crate::driver::RecoveredDescribeStreamsGroupCall>,
    raw_terminal: Option<DriverTerminal>,
    terminal: Option<DescribeStreamsGroupTerminal>,
}

pub(crate) struct DescribeStreamsGroupHost {
    operations: Vec<DescribeStreamsGroupOperation>,
    completions:
        CompletionRegistry<DescribeStreamsGroupTerminal, AdminDescribeStreamsGroupPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeStreamsGroupHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeStreamsGroupHost {
    pub(crate) fn new(publisher: AdminDescribeStreamsGroupPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_STREAMS_GROUP_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                DESCRIBE_STREAMS_GROUP_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_STREAMS_GROUP_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeStreamsGroupTurn, DescribeStreamsGroupHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeStreamsGroupTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeStreamsGroupTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DescribeStreamsGroupInput::DeadlineElapsed)?;
            return Ok(DescribeStreamsGroupTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeStreamsGroupHostError::MissingSubmission)?;
        self.operations[index].handoff = DescribeStreamsGroupHandoff::HandedOff;
        Ok(DescribeStreamsGroupTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeStreamsGroupCall,
    ) -> Result<(), DescribeStreamsGroupHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeStreamsGroupHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeStreamsGroupHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(DescribeStreamsGroupHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, DescribeStreamsGroupInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), DescribeStreamsGroupHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeStreamsGroupHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeStreamsGroupHandoff::HandedOff {
            return Err(DescribeStreamsGroupHostError::InvalidHandoff);
        }
        self.apply(operation_id, DescribeStreamsGroupInput::DriverRejected)
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
        input: DescribeStreamsGroupInput,
    ) -> Result<(), DescribeStreamsGroupHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeStreamsGroupHostError::UnknownOperation)?;
        let accepted = matches!(&input, DescribeStreamsGroupInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DescribeStreamsGroupHandoff::HandedOff {
            return Err(DescribeStreamsGroupHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DescribeStreamsGroupHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: DescribeStreamsGroupEffect,
    ) -> Result<(), DescribeStreamsGroupHostError> {
        let operation_id = self.operations[index].operation_id;
        match effect {
            DescribeStreamsGroupEffect::Submit {
                operation_id: effect_id,
                deadline,
                plan,
            } => {
                if effect_id != operation_id
                    || deadline != self.operations[index].deadline.core()
                    || plan.group_ids().len() != 1
                    || self.operations[index].submission.is_some()
                    || self.operations[index].active_plan.is_some()
                    || self.operations[index].call.is_some()
                    || self.operations[index].recovered_call.is_some()
                    || self.operations[index].raw_terminal.is_some()
                {
                    return Err(DescribeStreamsGroupHostError::SubmissionMismatch);
                }
                self.operations[index].active_plan = Some(plan.clone());
                self.operations[index].submission = Some(DescribeStreamsGroupSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    plan,
                    result_limit: self.operations[index].remaining_result_bytes,
                });
                self.operations[index].handoff = DescribeStreamsGroupHandoff::Untouched;
                Ok(())
            }
            DescribeStreamsGroupEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(DescribeStreamsGroupHostError::SubmissionMismatch);
                }
                self.operations[index].submission = None;
                self.operations[index].active_plan = None;
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
