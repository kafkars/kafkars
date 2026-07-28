//! Atomic completion and retained-byte reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    DELETE_CONSUMER_GROUPS_DIAGNOSTIC_BYTES, DeleteConsumerGroupsEffect, DeleteConsumerGroupsInput,
    DeleteConsumerGroupsMachine, DeleteConsumerGroupsPlan, Moment, OperationId,
};

use crate::protocol::admin::delete_groups::delete_consumer_groups_request_peak_charge;
use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DELETE_CONSUMER_GROUPS_CAPACITY, DELETE_CONSUMER_GROUPS_RETAINED_BYTES,
    DeleteConsumerGroupsAdmission, DeleteConsumerGroupsHandoff, DeleteConsumerGroupsHost,
    DeleteConsumerGroupsHostError, DeleteConsumerGroupsOperation,
};
use crate::admin::delete_consumer_groups::{
    DeleteConsumerGroupsAdmissionErrorKind, DeleteConsumerGroupsObserver,
};

impl DeleteConsumerGroupsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteConsumerGroupsPlan,
    ) -> Result<DeleteConsumerGroupsAdmission, DeleteConsumerGroupsAdmissionErrorKind> {
        if !self.accepting {
            return Err(DeleteConsumerGroupsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DELETE_CONSUMER_GROUPS_CAPACITY {
            return Err(DeleteConsumerGroupsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DeleteConsumerGroupsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(DeleteConsumerGroupsAdmissionErrorKind::RetainedBytes)?;
        let request_limit = plan
            .targets()
            .iter()
            .map(delete_consumer_groups_request_peak_charge)
            .try_fold(0usize, |maximum, charge| {
                charge.map(|charge| maximum.max(charge))
            })
            .ok_or(DeleteConsumerGroupsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = DELETE_CONSUMER_GROUPS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .and_then(|limit| limit.checked_sub(request_limit))
            .filter(|limit| *limit > 0)
            .ok_or(DeleteConsumerGroupsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(DELETE_CONSUMER_GROUPS_RETAINED_BYTES)
            .filter(|total| *total <= DELETE_CONSUMER_GROUPS_RETAINED_BYTES)
            .ok_or(DeleteConsumerGroupsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let expected_plan = plan.clone();
        let mut operation = DeleteConsumerGroupsOperation {
            operation_id,
            machine: DeleteConsumerGroupsMachine::new(operation_id, deadline.core(), plan),
            plan: expected_plan,
            completion_id,
            deadline,
            retained_bytes: DELETE_CONSUMER_GROUPS_RETAINED_BYTES,
            request_limit,
            result_limit: remaining_result_bytes,
            remaining_result_bytes,
            submission: None,
            rejected_submission: None,
            handoff: DeleteConsumerGroupsHandoff::Untouched,
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
        Ok(DeleteConsumerGroupsAdmission {
            observer: DeleteConsumerGroupsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DeleteConsumerGroupsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DeleteConsumerGroupsHostError> {
    let transition = operation
        .machine
        .apply(DeleteConsumerGroupsInput::Start { now })?;
    match transition.into_effect() {
        Some(DeleteConsumerGroupsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            target,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(DeleteConsumerGroupsHostError::SubmissionMismatch);
            }
            if operation.machine.current_target() != Some(&target)
                || !operation.plan.targets().contains(&target)
            {
                return Err(DeleteConsumerGroupsHostError::SubmissionMismatch);
            }
            operation.submission = Some(super::DeleteConsumerGroupsSubmission {
                operation_id,
                deadline,
                plan: operation.plan.clone(),
                target,
                request_limit: operation.request_limit,
                result_limit: operation.result_limit,
            });
            Ok(false)
        }
        Some(DeleteConsumerGroupsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DeleteConsumerGroupsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DeleteConsumerGroupsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DeleteConsumerGroupsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DeleteConsumerGroupsAdmissionErrorKind::Capacity,
        _ => DeleteConsumerGroupsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &DeleteConsumerGroupsPlan) -> Option<usize> {
    let group_id_bytes = plan.targets().iter().try_fold(0usize, |total, target| {
        total.checked_add(target.group_id().len())
    })?;
    size_of::<DeleteConsumerGroupsOperation>()
        .checked_add(size_of::<super::DeleteConsumerGroupsSubmission>())?
        .checked_add(3usize.checked_mul(size_of::<DeleteConsumerGroupsPlan>())?)?
        .checked_add(plan.targets().len().checked_mul(
            3usize.checked_mul(size_of::<kafka_client_core::DeleteConsumerGroupsTarget>())?,
        )?)?
        .checked_add(size_of::<kafka_client_core::DeleteConsumerGroupsTarget>())?
        .checked_add(
            plan.targets()
                .len()
                .checked_mul(size_of::<kafka_client_core::DeleteConsumerGroupsOutcome>())?,
        )?
        .checked_add(
            plan.targets()
                .len()
                .checked_mul(DELETE_CONSUMER_GROUPS_DIAGNOSTIC_BYTES)?,
        )?
        .checked_add(4usize.checked_mul(group_id_bytes)?)
}
