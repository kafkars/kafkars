//! Atomic completion and eight-MiB envelope reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    DescribeAclsEffect, DescribeAclsInput, DescribeAclsMachine, DescribeAclsPlan, Moment,
    OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DESCRIBE_ACLS_CAPACITY, DESCRIBE_ACLS_RETAINED_BYTES, DescribeAclsAdmission,
    DescribeAclsHandoff, DescribeAclsHost, DescribeAclsHostError, DescribeAclsOperation,
    DescribeAclsSubmission,
};
use crate::admin::describe_acls::{DescribeAclsAdmissionErrorKind, DescribeAclsObserver};

impl DescribeAclsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeAclsPlan,
    ) -> Result<DescribeAclsAdmission, DescribeAclsAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeAclsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DESCRIBE_ACLS_CAPACITY {
            return Err(DescribeAclsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeAclsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge =
            request_owner_charge(&plan).ok_or(DescribeAclsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = DESCRIBE_ACLS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(DescribeAclsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(DESCRIBE_ACLS_RETAINED_BYTES)
            .filter(|total| *total <= DESCRIBE_ACLS_RETAINED_BYTES)
            .ok_or(DescribeAclsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let host_plan = plan.clone();
        let mut operation = DescribeAclsOperation {
            operation_id,
            machine: DescribeAclsMachine::new(operation_id, deadline.core(), plan),
            plan: host_plan,
            completion_id,
            deadline,
            retained_bytes: DESCRIBE_ACLS_RETAINED_BYTES,
            result_limit: remaining_result_bytes,
            remaining_result_bytes,
            submission: None,
            handoff: DescribeAclsHandoff::Untouched,
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
        Ok(DescribeAclsAdmission {
            observer: DescribeAclsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeAclsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DescribeAclsHostError> {
    let transition = operation.machine.apply(DescribeAclsInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeAclsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id
                || core_deadline != deadline.core()
                || plan != operation.plan
            {
                return Err(DescribeAclsHostError::SubmissionMismatch);
            }
            operation.submission = Some(DescribeAclsSubmission {
                operation_id,
                deadline,
                plan,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(DescribeAclsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DescribeAclsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeAclsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeAclsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeAclsAdmissionErrorKind::Capacity,
        _ => DescribeAclsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &DescribeAclsPlan) -> Option<usize> {
    let filter = plan.filter();
    let string_bytes = filter
        .resource_name()
        .map_or(0, str::len)
        .checked_add(filter.principal().map_or(0, str::len))?
        .checked_add(filter.host().map_or(0, str::len))?;
    size_of::<DescribeAclsOperation>()
        .checked_add(size_of::<DescribeAclsSubmission>())?
        .checked_add(3usize.checked_mul(string_bytes)?)
}
