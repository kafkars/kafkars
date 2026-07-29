//! Atomic completion, request, and two-MiB result reservation.

use core::mem::size_of;

use kafka_client_core::{
    AlterShareGroupOffsetsEffect, AlterShareGroupOffsetsInput, AlterShareGroupOffsetsMachine,
    AlterShareGroupOffsetsPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ALTER_SHARE_GROUP_OFFSETS_CAPACITY, ALTER_SHARE_GROUP_OFFSETS_RESULT_BYTES,
    ALTER_SHARE_GROUP_OFFSETS_RETAINED_BYTES, AlterShareGroupOffsetsAdmission,
    AlterShareGroupOffsetsHandoff, AlterShareGroupOffsetsHost, AlterShareGroupOffsetsHostError,
    AlterShareGroupOffsetsOperation, AlterShareGroupOffsetsSubmission,
};
use crate::admin::alter_share_group_offsets::{
    AlterShareGroupOffsetsAdmissionErrorKind, AlterShareGroupOffsetsObserver,
};

impl AlterShareGroupOffsetsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterShareGroupOffsetsPlan,
    ) -> Result<AlterShareGroupOffsetsAdmission, AlterShareGroupOffsetsAdmissionErrorKind> {
        if !self.accepting {
            return Err(AlterShareGroupOffsetsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ALTER_SHARE_GROUP_OFFSETS_CAPACITY {
            return Err(AlterShareGroupOffsetsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AlterShareGroupOffsetsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(AlterShareGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let operation_bytes = owner_charge
            .checked_add(ALTER_SHARE_GROUP_OFFSETS_RESULT_BYTES)
            .ok_or(AlterShareGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(operation_bytes)
            .filter(|total| *total <= ALTER_SHARE_GROUP_OFFSETS_RETAINED_BYTES)
            .ok_or(AlterShareGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let correlation_plan = plan.clone();
        let mut operation = AlterShareGroupOffsetsOperation {
            operation_id,
            machine: AlterShareGroupOffsetsMachine::new(operation_id, deadline.core(), plan),
            plan: correlation_plan,
            completion_id,
            deadline,
            retained_bytes: operation_bytes,
            remaining_result_bytes: ALTER_SHARE_GROUP_OFFSETS_RESULT_BYTES,
            submission: None,
            handoff: AlterShareGroupOffsetsHandoff::Untouched,
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
        Ok(AlterShareGroupOffsetsAdmission {
            observer: AlterShareGroupOffsetsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut AlterShareGroupOffsetsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AlterShareGroupOffsetsHostError> {
    let transition = operation
        .machine
        .apply(AlterShareGroupOffsetsInput::Start { now })?;
    match transition.into_effect() {
        Some(AlterShareGroupOffsetsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(AlterShareGroupOffsetsHostError::SubmissionMismatch);
            }
            operation.submission = Some(AlterShareGroupOffsetsSubmission {
                operation_id,
                deadline,
                plan,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(AlterShareGroupOffsetsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(AlterShareGroupOffsetsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(AlterShareGroupOffsetsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> AlterShareGroupOffsetsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AlterShareGroupOffsetsAdmissionErrorKind::Capacity,
        _ => AlterShareGroupOffsetsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AlterShareGroupOffsetsPlan) -> Option<usize> {
    let text_bytes = plan
        .changes()
        .iter()
        .try_fold(plan.group_id().len(), |total, change| {
            total.checked_add(change.topic().len())
        })?;
    let vector_bytes = plan
        .changes()
        .len()
        .checked_mul(size_of::<kafka_client_core::AlterShareGroupOffset>())?;
    let duplicated_plan_bytes = text_bytes.checked_add(vector_bytes)?.checked_mul(3)?;
    size_of::<AlterShareGroupOffsetsOperation>()
        .checked_add(size_of::<AlterShareGroupOffsetsSubmission>())?
        .checked_add(duplicated_plan_bytes)
}
