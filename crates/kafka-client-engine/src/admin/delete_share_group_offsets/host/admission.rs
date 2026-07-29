//! Atomic completion, request, and two-MiB result reservation.

use core::mem::size_of;

use kafka_client_core::{
    DeleteShareGroupOffsetsEffect, DeleteShareGroupOffsetsInput, DeleteShareGroupOffsetsMachine,
    DeleteShareGroupOffsetsPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DELETE_SHARE_GROUP_OFFSETS_CAPACITY, DELETE_SHARE_GROUP_OFFSETS_RESULT_BYTES,
    DELETE_SHARE_GROUP_OFFSETS_RETAINED_BYTES, DeleteShareGroupOffsetsAdmission,
    DeleteShareGroupOffsetsHandoff, DeleteShareGroupOffsetsHost, DeleteShareGroupOffsetsHostError,
    DeleteShareGroupOffsetsOperation, DeleteShareGroupOffsetsSubmission,
};
use crate::admin::delete_share_group_offsets::{
    DeleteShareGroupOffsetsAdmissionErrorKind, DeleteShareGroupOffsetsObserver,
};

impl DeleteShareGroupOffsetsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteShareGroupOffsetsPlan,
    ) -> Result<DeleteShareGroupOffsetsAdmission, DeleteShareGroupOffsetsAdmissionErrorKind> {
        if !self.accepting {
            return Err(DeleteShareGroupOffsetsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DELETE_SHARE_GROUP_OFFSETS_CAPACITY {
            return Err(DeleteShareGroupOffsetsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DeleteShareGroupOffsetsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(DeleteShareGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let operation_bytes = owner_charge
            .checked_add(DELETE_SHARE_GROUP_OFFSETS_RESULT_BYTES)
            .ok_or(DeleteShareGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(operation_bytes)
            .filter(|total| *total <= DELETE_SHARE_GROUP_OFFSETS_RETAINED_BYTES)
            .ok_or(DeleteShareGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let correlation_plan = plan.clone();
        let mut operation = DeleteShareGroupOffsetsOperation {
            operation_id,
            machine: DeleteShareGroupOffsetsMachine::new(operation_id, deadline.core(), plan),
            plan: correlation_plan,
            completion_id,
            deadline,
            retained_bytes: operation_bytes,
            remaining_result_bytes: DELETE_SHARE_GROUP_OFFSETS_RESULT_BYTES,
            submission: None,
            handoff: DeleteShareGroupOffsetsHandoff::Untouched,
            call: None,
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
        if terminal_ready && let Err(error) = self.publish_terminal(self.operations.len() - 1) {
            self.health = Some(error);
            fault = Some(error);
        }
        Ok(DeleteShareGroupOffsetsAdmission {
            observer: DeleteShareGroupOffsetsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DeleteShareGroupOffsetsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DeleteShareGroupOffsetsHostError> {
    let transition = operation
        .machine
        .apply(DeleteShareGroupOffsetsInput::Start { now })?;
    match transition.into_effect() {
        Some(DeleteShareGroupOffsetsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(DeleteShareGroupOffsetsHostError::SubmissionMismatch);
            }
            operation.submission = Some(DeleteShareGroupOffsetsSubmission {
                operation_id,
                deadline,
                plan,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(DeleteShareGroupOffsetsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DeleteShareGroupOffsetsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DeleteShareGroupOffsetsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DeleteShareGroupOffsetsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DeleteShareGroupOffsetsAdmissionErrorKind::Capacity,
        _ => DeleteShareGroupOffsetsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &DeleteShareGroupOffsetsPlan) -> Option<usize> {
    let text_bytes = plan
        .topics()
        .iter()
        .try_fold(plan.group_id().len(), |total, topic| {
            total.checked_add(topic.len())
        })?;
    let vector_bytes = plan.topics().len().checked_mul(size_of::<String>())?;
    let duplicated_plan_bytes = text_bytes.checked_add(vector_bytes)?.checked_mul(3)?;
    size_of::<DeleteShareGroupOffsetsOperation>()
        .checked_add(size_of::<DeleteShareGroupOffsetsSubmission>())?
        .checked_add(duplicated_plan_bytes)
}
