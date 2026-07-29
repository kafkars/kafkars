//! Atomic completion and four-MiB envelope reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    AlterClientQuotaEntityComponent, AlterClientQuotaEntry, AlterClientQuotaOperation,
    AlterClientQuotasEffect, AlterClientQuotasInput, AlterClientQuotasMachine,
    AlterClientQuotasPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ALTER_CLIENT_QUOTAS_CAPACITY, ALTER_CLIENT_QUOTAS_RETAINED_BYTES, AlterClientQuotasAdmission,
    AlterClientQuotasHandoff, AlterClientQuotasHost, AlterClientQuotasHostError,
    AlterClientQuotasOperation, AlterClientQuotasSubmission,
};
use crate::admin::alter_client_quotas::{
    AlterClientQuotasAdmissionErrorKind, AlterClientQuotasObserver,
};

impl AlterClientQuotasHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterClientQuotasPlan,
    ) -> Result<AlterClientQuotasAdmission, AlterClientQuotasAdmissionErrorKind> {
        if !self.accepting {
            return Err(AlterClientQuotasAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ALTER_CLIENT_QUOTAS_CAPACITY {
            return Err(AlterClientQuotasAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AlterClientQuotasAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(AlterClientQuotasAdmissionErrorKind::RetainedBytes)?;
        let retained_limit = ALTER_CLIENT_QUOTAS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(AlterClientQuotasAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(ALTER_CLIENT_QUOTAS_RETAINED_BYTES)
            .filter(|total| *total <= ALTER_CLIENT_QUOTAS_RETAINED_BYTES)
            .ok_or(AlterClientQuotasAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let host_plan = plan.clone();
        let mut operation = AlterClientQuotasOperation {
            operation_id,
            machine: AlterClientQuotasMachine::new(operation_id, deadline.core(), plan),
            plan: host_plan,
            completion_id,
            deadline,
            retained_bytes: ALTER_CLIENT_QUOTAS_RETAINED_BYTES,
            retained_limit,
            submission: None,
            rejected_submission: None,
            handoff: AlterClientQuotasHandoff::Untouched,
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
        Ok(AlterClientQuotasAdmission {
            observer: AlterClientQuotasObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut AlterClientQuotasOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AlterClientQuotasHostError> {
    let transition = operation
        .machine
        .apply(AlterClientQuotasInput::Start { now })?;
    match transition.into_effect() {
        Some(AlterClientQuotasEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id
                || core_deadline != deadline.core()
                || operation.plan != plan
            {
                return Err(AlterClientQuotasHostError::SubmissionMismatch);
            }
            operation.submission = Some(AlterClientQuotasSubmission {
                operation_id,
                deadline,
                plan,
                retained_limit: operation.retained_limit,
            });
            Ok(false)
        }
        Some(AlterClientQuotasEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(AlterClientQuotasHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(AlterClientQuotasHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> AlterClientQuotasAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AlterClientQuotasAdmissionErrorKind::Capacity,
        _ => AlterClientQuotasAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AlterClientQuotasPlan) -> Option<usize> {
    let entry_storage = plan
        .entries()
        .len()
        .checked_mul(size_of::<AlterClientQuotaEntry>())?;
    let component_storage = plan.entries().iter().try_fold(0usize, |total, entry| {
        total.checked_add(
            entry
                .entity()
                .components()
                .len()
                .checked_mul(size_of::<AlterClientQuotaEntityComponent>())?,
        )
    })?;
    let operation_storage = plan.entries().iter().try_fold(0usize, |total, entry| {
        total.checked_add(
            entry
                .operations()
                .len()
                .checked_mul(size_of::<AlterClientQuotaOperation>())?,
        )
    })?;
    let string_bytes = plan.entries().iter().try_fold(0usize, |total, entry| {
        let component_bytes =
            entry
                .entity()
                .components()
                .iter()
                .try_fold(0usize, |bytes, component| {
                    bytes
                        .checked_add(component.entity_type().len())?
                        .checked_add(component.entity_name().map_or(0, str::len))
                })?;
        let key_bytes = entry
            .operations()
            .iter()
            .try_fold(0usize, |bytes, operation| {
                bytes.checked_add(operation.key().len())
            })?;
        total.checked_add(component_bytes)?.checked_add(key_bytes)
    })?;
    let plan_storage = entry_storage
        .checked_add(component_storage)?
        .checked_add(operation_storage)?
        .checked_add(string_bytes)?;
    size_of::<AlterClientQuotasOperation>()
        .checked_add(size_of::<AlterClientQuotasSubmission>())?
        .checked_add(3usize.checked_mul(plan_storage)?)
}
