//! Atomic completion and retained-byte reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    AdminListOffsetsEffect, AdminListOffsetsInput, AdminListOffsetsMachine, AdminListOffsetsPlan,
    Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ADMIN_LIST_OFFSETS_CAPACITY, ADMIN_LIST_OFFSETS_RETAINED_BYTES, AdminListOffsetsAdmission,
    AdminListOffsetsHandoff, AdminListOffsetsHost, AdminListOffsetsHostError,
    AdminListOffsetsOperation,
};
use crate::admin::list_offsets::{AdminListOffsetsAdmissionErrorKind, AdminListOffsetsObserver};

impl AdminListOffsetsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminListOffsetsPlan,
    ) -> Result<AdminListOffsetsAdmission, AdminListOffsetsAdmissionErrorKind> {
        if !self.accepting {
            return Err(AdminListOffsetsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ADMIN_LIST_OFFSETS_CAPACITY {
            return Err(AdminListOffsetsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AdminListOffsetsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge =
            request_owner_charge(&plan).ok_or(AdminListOffsetsAdmissionErrorKind::RetainedBytes)?;
        if owner_charge >= ADMIN_LIST_OFFSETS_RETAINED_BYTES {
            return Err(AdminListOffsetsAdmissionErrorKind::RetainedBytes);
        }
        let total_bytes = self
            .retained_bytes
            .checked_add(ADMIN_LIST_OFFSETS_RETAINED_BYTES)
            .filter(|total| *total <= ADMIN_LIST_OFFSETS_RETAINED_BYTES)
            .ok_or(AdminListOffsetsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = AdminListOffsetsOperation {
            operation_id,
            machine: AdminListOffsetsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: ADMIN_LIST_OFFSETS_RETAINED_BYTES,
            submission: None,
            handoff: AdminListOffsetsHandoff::Untouched,
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
        Ok(AdminListOffsetsAdmission {
            observer: AdminListOffsetsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut AdminListOffsetsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AdminListOffsetsHostError> {
    let transition = operation
        .machine
        .apply(AdminListOffsetsInput::Start { now })?;
    match transition.into_effect() {
        Some(AdminListOffsetsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            target,
            read_isolation,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(AdminListOffsetsHostError::SubmissionMismatch);
            }
            operation.submission = Some(super::AdminListOffsetsSubmission {
                operation_id,
                deadline,
                target,
                read_isolation,
            });
            Ok(false)
        }
        Some(AdminListOffsetsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(AdminListOffsetsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(AdminListOffsetsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> AdminListOffsetsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AdminListOffsetsAdmissionErrorKind::Capacity,
        _ => AdminListOffsetsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AdminListOffsetsPlan) -> Option<usize> {
    let topic_bytes = plan.targets().iter().try_fold(0usize, |total, target| {
        total.checked_add(target.topic().len())
    })?;
    size_of::<AdminListOffsetsOperation>()
        .checked_add(size_of::<super::AdminListOffsetsSubmission>())?
        .checked_add(2usize.checked_mul(size_of::<AdminListOffsetsPlan>())?)?
        .checked_add(
            plan.targets()
                .len()
                .checked_mul(size_of::<kafka_client_core::AdminListOffsetTarget>())?,
        )?
        .checked_add(
            plan.targets()
                .len()
                .checked_mul(size_of::<kafka_client_core::AdminListOffsetOutcome>())?,
        )?
        .checked_add(2usize.checked_mul(topic_bytes)?)
}
