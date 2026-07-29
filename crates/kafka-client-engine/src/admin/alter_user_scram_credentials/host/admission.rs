//! Atomic terminal, prepared-request, and four-MiB envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    AlterUserScramCredentialChange, AlterUserScramCredentialsEffect,
    AlterUserScramCredentialsInput, AlterUserScramCredentialsMachine,
    AlterUserScramCredentialsPlan, Moment, OperationId,
};

use crate::{
    clock::OperationDeadline, completion::CompletionRegistryError,
    protocol::admin::alter_user_scram_credentials::PreparedAlterUserScramCredentialsRequest,
};

use super::{
    ALTER_USER_SCRAM_CREDENTIALS_CAPACITY, ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES,
    AlterUserScramCredentialsAdmission, AlterUserScramCredentialsHandoff,
    AlterUserScramCredentialsHost, AlterUserScramCredentialsHostError,
    AlterUserScramCredentialsOperation, AlterUserScramCredentialsSubmission,
};
use crate::admin::alter_user_scram_credentials::{
    AlterUserScramCredentialsAdmissionErrorKind, AlterUserScramCredentialsObserver,
};

impl AlterUserScramCredentialsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterUserScramCredentialsPlan,
        prepared_request: PreparedAlterUserScramCredentialsRequest,
    ) -> Result<AlterUserScramCredentialsAdmission, AlterUserScramCredentialsAdmissionErrorKind>
    {
        if !self.accepting {
            return Err(AlterUserScramCredentialsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ALTER_USER_SCRAM_CREDENTIALS_CAPACITY {
            return Err(AlterUserScramCredentialsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AlterUserScramCredentialsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan, &prepared_request)
            .ok_or(AlterUserScramCredentialsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(AlterUserScramCredentialsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES)
            .filter(|total| *total <= ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES)
            .ok_or(AlterUserScramCredentialsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = AlterUserScramCredentialsOperation {
            operation_id,
            machine: AlterUserScramCredentialsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES,
            remaining_result_bytes,
            submission: None,
            handoff: AlterUserScramCredentialsHandoff::Untouched,
            call: None,
            raw_terminal: None,
            terminal: None,
        };
        let start_result = start(&mut operation, now, deadline, prepared_request);
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
        Ok(AlterUserScramCredentialsAdmission {
            observer: AlterUserScramCredentialsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut AlterUserScramCredentialsOperation,
    now: Moment,
    deadline: OperationDeadline,
    prepared_request: PreparedAlterUserScramCredentialsRequest,
) -> Result<bool, AlterUserScramCredentialsHostError> {
    let transition = operation
        .machine
        .apply(AlterUserScramCredentialsInput::Start { now })?;
    match transition.into_effect() {
        Some(AlterUserScramCredentialsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(AlterUserScramCredentialsHostError::SubmissionMismatch);
            }
            operation.submission = Some(AlterUserScramCredentialsSubmission {
                operation_id,
                deadline,
                plan,
                prepared_request,
            });
            Ok(false)
        }
        Some(AlterUserScramCredentialsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(AlterUserScramCredentialsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(AlterUserScramCredentialsHostError::MissingSubmission),
    }
}

fn reservation_error(
    error: CompletionRegistryError,
) -> AlterUserScramCredentialsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AlterUserScramCredentialsAdmissionErrorKind::Capacity,
        _ => AlterUserScramCredentialsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(
    plan: &AlterUserScramCredentialsPlan,
    prepared: &PreparedAlterUserScramCredentialsRequest,
) -> Option<usize> {
    let change_storage = plan
        .changes()
        .len()
        .checked_mul(size_of::<AlterUserScramCredentialChange>())?;
    let change_user_bytes = plan.changes().iter().try_fold(0usize, |bytes, change| {
        bytes.checked_add(change.user().len())
    })?;
    let affected_storage = plan.changes().len().checked_mul(size_of::<String>())?;
    let affected_user_bytes = plan
        .affected_users()
        .iter()
        .try_fold(0usize, |bytes, user| bytes.checked_add(user.len()))?;
    let one_plan = change_storage
        .checked_add(change_user_bytes)?
        .checked_add(affected_storage)?
        .checked_add(affected_user_bytes)?;
    size_of::<AlterUserScramCredentialsOperation>()
        .checked_add(size_of::<AlterUserScramCredentialsSubmission>())?
        .checked_add(one_plan.checked_mul(2)?)?
        .checked_add(prepared.retained_heap_bytes())
}
