//! Atomic completion and retained-byte reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    DeleteRecordsEffect, DeleteRecordsInput, DeleteRecordsMachine, DeleteRecordsPlan, Moment,
    OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DELETE_RECORDS_CAPACITY, DELETE_RECORDS_RETAINED_BYTES, DeleteRecordsAdmission,
    DeleteRecordsHandoff, DeleteRecordsHost, DeleteRecordsHostError, DeleteRecordsOperation,
};
use crate::admin::delete_records::{DeleteRecordsAdmissionErrorKind, DeleteRecordsObserver};

impl DeleteRecordsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteRecordsPlan,
    ) -> Result<DeleteRecordsAdmission, DeleteRecordsAdmissionErrorKind> {
        if !self.accepting {
            return Err(DeleteRecordsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DELETE_RECORDS_CAPACITY {
            return Err(DeleteRecordsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DeleteRecordsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge =
            request_owner_charge(&plan).ok_or(DeleteRecordsAdmissionErrorKind::RetainedBytes)?;
        if owner_charge >= DELETE_RECORDS_RETAINED_BYTES {
            return Err(DeleteRecordsAdmissionErrorKind::RetainedBytes);
        }
        let total_bytes = self
            .retained_bytes
            .checked_add(DELETE_RECORDS_RETAINED_BYTES)
            .filter(|total| *total <= DELETE_RECORDS_RETAINED_BYTES)
            .ok_or(DeleteRecordsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = DeleteRecordsOperation {
            operation_id,
            machine: DeleteRecordsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: DELETE_RECORDS_RETAINED_BYTES,
            submission: None,
            handoff: DeleteRecordsHandoff::Untouched,
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
        Ok(DeleteRecordsAdmission {
            observer: DeleteRecordsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DeleteRecordsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DeleteRecordsHostError> {
    let transition = operation.machine.apply(DeleteRecordsInput::Start { now })?;
    match transition.into_effect() {
        Some(DeleteRecordsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            target,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(DeleteRecordsHostError::SubmissionMismatch);
            }
            operation.submission = Some(super::DeleteRecordsSubmission {
                operation_id,
                deadline,
                target,
            });
            Ok(false)
        }
        Some(DeleteRecordsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DeleteRecordsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DeleteRecordsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DeleteRecordsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DeleteRecordsAdmissionErrorKind::Capacity,
        _ => DeleteRecordsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &DeleteRecordsPlan) -> Option<usize> {
    let topic_bytes = plan.targets().iter().try_fold(0usize, |total, target| {
        total.checked_add(target.topic().len())
    })?;
    size_of::<DeleteRecordsOperation>()
        .checked_add(size_of::<super::DeleteRecordsSubmission>())?
        .checked_add(2usize.checked_mul(size_of::<DeleteRecordsPlan>())?)?
        .checked_add(
            plan.targets()
                .len()
                .checked_mul(size_of::<kafka_client_core::DeleteRecordsTarget>())?,
        )?
        .checked_add(
            plan.targets()
                .len()
                .checked_mul(size_of::<kafka_client_core::DeleteRecordsOutcome>())?,
        )?
        .checked_add(2usize.checked_mul(topic_bytes)?)
}
