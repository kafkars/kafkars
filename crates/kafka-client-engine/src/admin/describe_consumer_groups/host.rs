//! Bounded ownership of consumer-group description machines and coordinator calls.

mod admission;
mod model;
mod modern_response;
mod recovered;
mod response;
mod terminal;

#[cfg(test)]
mod ownership_test;

use kafka_client_core::{
    AdminDescribeConsumerGroupsEffect, AdminDescribeConsumerGroupsInput,
    AdminDescribeConsumerGroupsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeConsumerGroupsPublisher,
    completion::{CompletionId, CompletionRegistry},
};

use super::{DescribeConsumerGroupsHostError, DescribeConsumerGroupsObserver};
use model::{
    DescribeConsumerGroupsAttempt, DescribeConsumerGroupsAttemptBounds,
    DescribeConsumerGroupsHandoff, DescribeConsumerGroupsOperation,
};
pub(crate) use model::{DescribeConsumerGroupsSubmission, DescribeConsumerGroupsTurn};

pub(crate) const DESCRIBE_CONSUMER_GROUPS_CAPACITY: usize = 16;
const DESCRIBE_CONSUMER_GROUPS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DescribeConsumerGroupsAdmission {
    pub(crate) observer: DescribeConsumerGroupsObserver,
    pub(crate) fault: Option<DescribeConsumerGroupsHostError>,
}

pub(crate) struct DescribeConsumerGroupsHost {
    operations: Vec<DescribeConsumerGroupsOperation>,
    completions: CompletionRegistry<
        AdminDescribeConsumerGroupsTerminal,
        AdminDescribeConsumerGroupsPublisher,
    >,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeConsumerGroupsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeConsumerGroupsHost {
    pub(crate) fn new(publisher: AdminDescribeConsumerGroupsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_CONSUMER_GROUPS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                DESCRIBE_CONSUMER_GROUPS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_CONSUMER_GROUPS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeConsumerGroupsTurn, DescribeConsumerGroupsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeConsumerGroupsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeConsumerGroupsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(
                operation_id,
                AdminDescribeConsumerGroupsInput::DeadlineElapsed,
            )?;
            return Ok(DescribeConsumerGroupsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeConsumerGroupsHostError::MissingSubmission)?;
        self.operations[index].handoff = DescribeConsumerGroupsHandoff::HandedOff;
        Ok(DescribeConsumerGroupsTurn::Submit(submission))
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
        input: AdminDescribeConsumerGroupsInput,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeConsumerGroupsHostError::UnknownOperation)?;
        let accepted = matches!(&input, AdminDescribeConsumerGroupsInput::DriverAccepted);
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DescribeConsumerGroupsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: AdminDescribeConsumerGroupsEffect,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        let operation_id = self.operations[index].operation_id;
        match effect {
            AdminDescribeConsumerGroupsEffect::Submit {
                operation_id: effect_id,
                deadline,
                group_id,
                include_authorized_operations,
                call_kind,
            } => {
                if effect_id != operation_id || deadline != self.operations[index].deadline.core() {
                    return Err(DescribeConsumerGroupsHostError::SubmissionMismatch);
                }
                let route_index = match self.operations[index].attempt.as_ref() {
                    Some(attempt) if attempt.group_id == group_id => {
                        self.operations[index].route_index
                    }
                    Some(_) => self.operations[index]
                        .route_index
                        .checked_add(1)
                        .ok_or(DescribeConsumerGroupsHostError::SubmissionMismatch)?,
                    None => self.operations[index].route_index,
                };
                if self.operations[index].route_plan.group(route_index) != Some(group_id.as_str())
                    || self.operations[index]
                        .route_plan
                        .include_authorized_operations()
                        != include_authorized_operations
                    || self.operations[index].machine.current_group() != Some(group_id.as_str())
                    || self.operations[index].machine.call_kind() != call_kind
                {
                    return Err(DescribeConsumerGroupsHostError::SubmissionMismatch);
                }
                let bounds = DescribeConsumerGroupsAttemptBounds {
                    request_scratch_limit: self.operations[index].remaining_result_bytes,
                    result_limit: self.operations[index].remaining_result_bytes,
                };
                self.operations[index].route_index = route_index;
                self.operations[index].attempt = Some(DescribeConsumerGroupsAttempt {
                    group_id: group_id.clone(),
                    include_authorized_operations,
                    call_kind,
                    bounds,
                });
                self.operations[index].submission = Some(DescribeConsumerGroupsSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    group_id,
                    include_authorized_operations,
                    call_kind,
                    bounds,
                });
                self.operations[index].handoff = DescribeConsumerGroupsHandoff::Untouched;
                Ok(())
            }
            AdminDescribeConsumerGroupsEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(DescribeConsumerGroupsHostError::SubmissionMismatch);
                }
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
        }
    }
}
