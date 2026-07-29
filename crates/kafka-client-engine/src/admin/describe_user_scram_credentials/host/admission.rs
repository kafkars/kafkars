//! Atomic completion and four-MiB envelope reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    DescribeUserScramCredentialsEffect, DescribeUserScramCredentialsInput,
    DescribeUserScramCredentialsMachine, DescribeUserScramCredentialsPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY, DESCRIBE_USER_SCRAM_CREDENTIALS_RETAINED_BYTES,
    DescribeUserScramCredentialsAdmission, DescribeUserScramCredentialsHandoff,
    DescribeUserScramCredentialsHost, DescribeUserScramCredentialsHostError,
    DescribeUserScramCredentialsOperation, DescribeUserScramCredentialsSubmission,
    model::DescribeUserScramCredentialsAttemptBounds,
};
use crate::admin::describe_user_scram_credentials::{
    DescribeUserScramCredentialsAdmissionErrorKind, DescribeUserScramCredentialsObserver,
};

impl DescribeUserScramCredentialsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeUserScramCredentialsPlan,
    ) -> Result<DescribeUserScramCredentialsAdmission, DescribeUserScramCredentialsAdmissionErrorKind>
    {
        if !self.accepting {
            return Err(DescribeUserScramCredentialsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY {
            return Err(DescribeUserScramCredentialsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeUserScramCredentialsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(DescribeUserScramCredentialsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = DESCRIBE_USER_SCRAM_CREDENTIALS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(DescribeUserScramCredentialsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(DESCRIBE_USER_SCRAM_CREDENTIALS_RETAINED_BYTES)
            .filter(|total| *total <= DESCRIBE_USER_SCRAM_CREDENTIALS_RETAINED_BYTES)
            .ok_or(DescribeUserScramCredentialsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let bounds = DescribeUserScramCredentialsAttemptBounds {
            request_limit: remaining_result_bytes,
            result_limit: remaining_result_bytes,
        };
        let mut operation = DescribeUserScramCredentialsOperation {
            operation_id,
            machine: DescribeUserScramCredentialsMachine::new(
                operation_id,
                deadline.core(),
                plan.clone(),
            ),
            expected_plan: plan,
            completion_id,
            deadline,
            retained_bytes: DESCRIBE_USER_SCRAM_CREDENTIALS_RETAINED_BYTES,
            bounds,
            remaining_result_bytes,
            submission: None,
            handoff: DescribeUserScramCredentialsHandoff::Untouched,
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
        Ok(DescribeUserScramCredentialsAdmission {
            observer: DescribeUserScramCredentialsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeUserScramCredentialsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DescribeUserScramCredentialsHostError> {
    let transition = operation
        .machine
        .apply(DescribeUserScramCredentialsInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeUserScramCredentialsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id
                || core_deadline != deadline.core()
                || plan != operation.expected_plan
            {
                return Err(DescribeUserScramCredentialsHostError::SubmissionMismatch);
            }
            operation.submission = Some(DescribeUserScramCredentialsSubmission {
                operation_id,
                deadline,
                plan,
                bounds: operation.bounds,
            });
            Ok(false)
        }
        Some(DescribeUserScramCredentialsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DescribeUserScramCredentialsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeUserScramCredentialsHostError::MissingSubmission),
    }
}

fn reservation_error(
    error: CompletionRegistryError,
) -> DescribeUserScramCredentialsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeUserScramCredentialsAdmissionErrorKind::Capacity,
        _ => DescribeUserScramCredentialsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &DescribeUserScramCredentialsPlan) -> Option<usize> {
    let users = plan.users().unwrap_or_default();
    let selection_storage = users.len().checked_mul(size_of::<String>())?.checked_add(
        users
            .iter()
            .try_fold(0usize, |bytes, user| bytes.checked_add(user.len()))?,
    )?;
    size_of::<DescribeUserScramCredentialsOperation>()
        .checked_add(size_of::<DescribeUserScramCredentialsSubmission>())?
        .checked_add(3usize.checked_mul(selection_storage)?)
}
