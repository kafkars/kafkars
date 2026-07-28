//! Atomic terminal and retained-byte reservation before machine construction.

use core::mem::size_of;

use kafka_client_core::{
    AdminDescribeConsumerGroupsEffect, AdminDescribeConsumerGroupsInput,
    AdminDescribeConsumerGroupsMachine, AdminDescribeConsumerGroupsPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::model::DescribeConsumerGroupsRoutePlan;
use super::{
    DESCRIBE_CONSUMER_GROUPS_CAPACITY, DESCRIBE_CONSUMER_GROUPS_RETAINED_BYTES,
    DescribeConsumerGroupsAdmission, DescribeConsumerGroupsAttempt,
    DescribeConsumerGroupsAttemptBounds, DescribeConsumerGroupsHandoff, DescribeConsumerGroupsHost,
    DescribeConsumerGroupsHostError, DescribeConsumerGroupsOperation,
    DescribeConsumerGroupsSubmission,
};
use crate::admin::describe_consumer_groups::{
    DescribeConsumerGroupsAdmissionErrorKind, DescribeConsumerGroupsObserver,
};

impl DescribeConsumerGroupsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminDescribeConsumerGroupsPlan,
    ) -> Result<DescribeConsumerGroupsAdmission, DescribeConsumerGroupsAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeConsumerGroupsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DESCRIBE_CONSUMER_GROUPS_CAPACITY {
            return Err(DescribeConsumerGroupsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeConsumerGroupsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(DescribeConsumerGroupsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = DESCRIBE_CONSUMER_GROUPS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .ok_or(DescribeConsumerGroupsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(DESCRIBE_CONSUMER_GROUPS_RETAINED_BYTES)
            .filter(|total| *total <= DESCRIBE_CONSUMER_GROUPS_RETAINED_BYTES)
            .ok_or(DescribeConsumerGroupsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let route_plan = DescribeConsumerGroupsRoutePlan::from_plan(&plan);
        let mut operation = DescribeConsumerGroupsOperation {
            operation_id,
            machine: AdminDescribeConsumerGroupsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: DESCRIBE_CONSUMER_GROUPS_RETAINED_BYTES,
            remaining_result_bytes,
            route_plan,
            route_index: 0,
            submission: None,
            attempt: None,
            handoff: DescribeConsumerGroupsHandoff::Untouched,
            call: None,
            recovered_call: None,
            raw_terminal: None,
            terminal: None,
        };
        let start_result = start(&mut operation, now, deadline);
        let terminal_ready = matches!(start_result, Ok(true));
        let mut fault = start_result.err();
        if let Some(error) = fault {
            self.health = Some(error);
        }
        self.operations.push(operation);
        if terminal_ready {
            if let Err(error) = self.publish_terminal(self.operations.len() - 1) {
                self.health = Some(error);
                fault = Some(error);
            }
        }
        Ok(DescribeConsumerGroupsAdmission {
            observer: DescribeConsumerGroupsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeConsumerGroupsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DescribeConsumerGroupsHostError> {
    let transition = operation
        .machine
        .apply(AdminDescribeConsumerGroupsInput::Start { now })?;
    match transition.into_effect() {
        Some(AdminDescribeConsumerGroupsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            group_id,
            include_authorized_operations,
            call_kind,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(DescribeConsumerGroupsHostError::SubmissionMismatch);
            }
            if operation.route_plan.group(0) != Some(group_id.as_str())
                || operation.route_plan.include_authorized_operations()
                    != include_authorized_operations
                || operation.machine.current_group() != Some(group_id.as_str())
                || operation.machine.call_kind() != call_kind
            {
                return Err(DescribeConsumerGroupsHostError::SubmissionMismatch);
            }
            let bounds = DescribeConsumerGroupsAttemptBounds {
                request_scratch_limit: operation.remaining_result_bytes,
                result_limit: operation.remaining_result_bytes,
            };
            operation.attempt = Some(DescribeConsumerGroupsAttempt {
                group_id: group_id.clone(),
                include_authorized_operations,
                call_kind,
                bounds,
            });
            operation.submission = Some(DescribeConsumerGroupsSubmission {
                operation_id,
                deadline,
                group_id,
                include_authorized_operations,
                call_kind,
                bounds,
            });
            Ok(false)
        }
        Some(AdminDescribeConsumerGroupsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DescribeConsumerGroupsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeConsumerGroupsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeConsumerGroupsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeConsumerGroupsAdmissionErrorKind::Capacity,
        _ => DescribeConsumerGroupsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AdminDescribeConsumerGroupsPlan) -> Option<usize> {
    let group_bytes = plan
        .groups()
        .iter()
        .try_fold(0usize, |total, group| total.checked_add(group.len()))?;
    size_of::<DescribeConsumerGroupsOperation>()
        .checked_add(size_of::<DescribeConsumerGroupsRoutePlan>())?
        .checked_add(size_of::<DescribeConsumerGroupsAttempt>())?
        .checked_add(size_of::<DescribeConsumerGroupsSubmission>())?
        .checked_add(
            plan.groups()
                .len()
                .checked_mul(4usize.checked_mul(size_of::<String>())?)?,
        )?
        .checked_add(4usize.checked_mul(group_bytes)?)
}
