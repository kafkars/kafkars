//! Atomic completion and four-MiB envelope reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    ClientQuotaMatch, DescribeClientQuotaFilterComponent, DescribeClientQuotasEffect,
    DescribeClientQuotasInput, DescribeClientQuotasMachine, DescribeClientQuotasPlan, Moment,
    OperationId,
};

use crate::{
    clock::OperationDeadline, completion::CompletionRegistryError,
    protocol::admin::describe_client_quotas::plan_request_peak_charge,
};

use super::{
    DESCRIBE_CLIENT_QUOTAS_CAPACITY, DESCRIBE_CLIENT_QUOTAS_RETAINED_BYTES,
    DescribeClientQuotasAdmission, DescribeClientQuotasHandoff, DescribeClientQuotasHost,
    DescribeClientQuotasHostError, DescribeClientQuotasOperation, DescribeClientQuotasSubmission,
};
use crate::admin::describe_client_quotas::{
    DescribeClientQuotasAdmissionErrorKind, DescribeClientQuotasObserver,
};

impl DescribeClientQuotasHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeClientQuotasPlan,
    ) -> Result<DescribeClientQuotasAdmission, DescribeClientQuotasAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeClientQuotasAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DESCRIBE_CLIENT_QUOTAS_CAPACITY {
            return Err(DescribeClientQuotasAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeClientQuotasAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(DescribeClientQuotasAdmissionErrorKind::RetainedBytes)?;
        let request_scratch_limit = plan_request_peak_charge(&plan)
            .ok_or(DescribeClientQuotasAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = DESCRIBE_CLIENT_QUOTAS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .and_then(|limit| limit.checked_sub(request_scratch_limit))
            .filter(|limit| *limit > 0)
            .ok_or(DescribeClientQuotasAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(DESCRIBE_CLIENT_QUOTAS_RETAINED_BYTES)
            .filter(|total| *total <= DESCRIBE_CLIENT_QUOTAS_RETAINED_BYTES)
            .ok_or(DescribeClientQuotasAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let expected_plan = plan.clone();
        let mut operation = DescribeClientQuotasOperation {
            operation_id,
            machine: DescribeClientQuotasMachine::new(operation_id, deadline.core(), plan),
            plan: expected_plan,
            completion_id,
            deadline,
            retained_bytes: DESCRIBE_CLIENT_QUOTAS_RETAINED_BYTES,
            request_scratch_limit,
            result_limit: remaining_result_bytes,
            remaining_result_bytes,
            submission: None,
            rejected_submission: None,
            handoff: DescribeClientQuotasHandoff::Untouched,
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
        Ok(DescribeClientQuotasAdmission {
            observer: DescribeClientQuotasObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeClientQuotasOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DescribeClientQuotasHostError> {
    let transition = operation
        .machine
        .apply(DescribeClientQuotasInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeClientQuotasEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(DescribeClientQuotasHostError::SubmissionMismatch);
            }
            if plan != operation.plan {
                return Err(DescribeClientQuotasHostError::SubmissionMismatch);
            }
            operation.submission = Some(DescribeClientQuotasSubmission {
                operation_id,
                deadline,
                plan,
                request_scratch_limit: operation.request_scratch_limit,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(DescribeClientQuotasEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DescribeClientQuotasHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeClientQuotasHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeClientQuotasAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeClientQuotasAdmissionErrorKind::Capacity,
        _ => DescribeClientQuotasAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &DescribeClientQuotasPlan) -> Option<usize> {
    let component_storage = plan
        .components()
        .len()
        .checked_mul(size_of::<DescribeClientQuotaFilterComponent>())?;
    let string_bytes = plan
        .components()
        .iter()
        .try_fold(0usize, |total, component| {
            let exact_name_bytes = match component.match_kind() {
                ClientQuotaMatch::Exact(name) => name.len(),
                ClientQuotaMatch::Default | ClientQuotaMatch::AnySpecified => 0,
            };
            total
                .checked_add(component.entity_type().len())?
                .checked_add(exact_name_bytes)
        })?;
    let plan_storage = component_storage.checked_add(string_bytes)?;
    size_of::<DescribeClientQuotasOperation>()
        .checked_add(size_of::<DescribeClientQuotasSubmission>())?
        .checked_add(3usize.checked_mul(plan_storage)?)
}
