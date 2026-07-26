//! Atomic completion, combined-envelope reservation, and linear submission handoff.

use core::mem::size_of;

use kafka_client_core::{
    DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsEffect,
    DeleteConsumerGroupOffsetsInput, DeleteConsumerGroupOffsetsMachine,
    DeleteConsumerGroupOffsetsPlan, Moment, OperationId,
};

use crate::{
    clock::OperationDeadline, completion::CompletionRegistryError,
    protocol::admin::group_offset_delete::OffsetDeleteTargetRef,
};

use super::{
    DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY, DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES,
    DeleteConsumerGroupOffsetsAdmission, DeleteConsumerGroupOffsetsHandoff,
    DeleteConsumerGroupOffsetsHost, DeleteConsumerGroupOffsetsHostError,
    DeleteConsumerGroupOffsetsOperation, DeleteConsumerGroupOffsetsSubmission,
};
use crate::admin::group_offset_delete::{
    DeleteConsumerGroupOffsetsAdmissionErrorKind, DeleteConsumerGroupOffsetsObserver,
};

impl DeleteConsumerGroupOffsetsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DeleteConsumerGroupOffsetsPlan,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.request_scratch_limit,
        )
    }
}

impl DeleteConsumerGroupOffsetsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteConsumerGroupOffsetsPlan,
    ) -> Result<DeleteConsumerGroupOffsetsAdmission, DeleteConsumerGroupOffsetsAdmissionErrorKind>
    {
        if !self.accepting {
            return Err(DeleteConsumerGroupOffsetsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY {
            return Err(DeleteConsumerGroupOffsetsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DeleteConsumerGroupOffsetsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(DeleteConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let target_refs_charge = target_refs_charge(plan.targets().len())
            .ok_or(DeleteConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let operation_limit = DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .and_then(|limit| limit.checked_sub(target_refs_charge))
            .filter(|limit| *limit > 0)
            .ok_or(DeleteConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES)
            .filter(|total| *total <= DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES)
            .ok_or(DeleteConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let response_plan = plan.clone();
        let mut operation = DeleteConsumerGroupOffsetsOperation {
            operation_id,
            machine: DeleteConsumerGroupOffsetsMachine::new(operation_id, deadline.core(), plan),
            response_plan,
            completion_id,
            deadline,
            retained_bytes: DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES,
            result_limit: operation_limit,
            submission: None,
            handoff: DeleteConsumerGroupOffsetsHandoff::Untouched,
            call: None,
            raw_terminal: None,
            terminal: None,
        };
        let start_result = start(&mut operation, now, deadline, operation_limit);
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
        Ok(DeleteConsumerGroupOffsetsAdmission {
            observer: DeleteConsumerGroupOffsetsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DeleteConsumerGroupOffsetsOperation,
    now: Moment,
    deadline: OperationDeadline,
    request_scratch_limit: usize,
) -> Result<bool, DeleteConsumerGroupOffsetsHostError> {
    let transition = operation
        .machine
        .apply(DeleteConsumerGroupOffsetsInput::Start { now })?;
    match transition.into_effect() {
        Some(DeleteConsumerGroupOffsetsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(DeleteConsumerGroupOffsetsHostError::SubmissionMismatch);
            }
            operation.submission = Some(DeleteConsumerGroupOffsetsSubmission {
                operation_id,
                deadline,
                plan,
                request_scratch_limit,
            });
            Ok(false)
        }
        Some(DeleteConsumerGroupOffsetsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DeleteConsumerGroupOffsetsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DeleteConsumerGroupOffsetsHostError::MissingSubmission),
    }
}

fn reservation_error(
    error: CompletionRegistryError,
) -> DeleteConsumerGroupOffsetsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DeleteConsumerGroupOffsetsAdmissionErrorKind::Capacity,
        _ => DeleteConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &DeleteConsumerGroupOffsetsPlan) -> Option<usize> {
    let text_bytes = plan
        .targets()
        .iter()
        .try_fold(plan.group_id().len(), |bytes, target| {
            bytes.checked_add(target.topic().len())
        })?;
    size_of::<DeleteConsumerGroupOffsetsOperation>()
        .checked_add(size_of::<DeleteConsumerGroupOffsetsSubmission>())?
        .checked_add(3usize.checked_mul(size_of::<DeleteConsumerGroupOffsetsPlan>())?)?
        .checked_add(
            3usize.checked_mul(
                plan.targets()
                    .len()
                    .checked_mul(size_of::<DeleteConsumerGroupOffsetTarget>())?,
            )?,
        )?
        .checked_add(3usize.checked_mul(text_bytes)?)
}

fn target_refs_charge(target_count: usize) -> Option<usize> {
    size_of::<Vec<OffsetDeleteTargetRef<'static>>>()
        .checked_add(target_count.checked_mul(size_of::<OffsetDeleteTargetRef<'static>>())?)
}
