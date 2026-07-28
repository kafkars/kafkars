//! Atomic terminal, result-vector, and eight-MiB envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    CreateAclBinding, CreateAclResult, CreateAclsEffect, CreateAclsInput, CreateAclsMachine,
    CreateAclsPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    CREATE_ACLS_CAPACITY, CREATE_ACLS_RETAINED_BYTES, CreateAclsAdmission, CreateAclsHandoff,
    CreateAclsHost, CreateAclsHostError, CreateAclsOperation, CreateAclsSubmission,
};
use crate::admin::create_acls::{CreateAclsAdmissionErrorKind, CreateAclsObserver};
use crate::admin::{CreateAclOutcome, CreateAclsBatch};

impl CreateAclsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: CreateAclsPlan,
    ) -> Result<CreateAclsAdmission, CreateAclsAdmissionErrorKind> {
        if !self.accepting {
            return Err(CreateAclsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= CREATE_ACLS_CAPACITY {
            return Err(CreateAclsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(CreateAclsAdmissionErrorKind::IdentityExhausted)?;
        let mut prepared_results = Vec::new();
        prepared_results
            .try_reserve_exact(plan.required_result_capacity())
            .map_err(|_error| CreateAclsAdmissionErrorKind::RetainedBytes)?;
        let prepared_outcomes =
            CreateAclsBatch::try_prepare_outcomes(plan.required_result_capacity())
                .map_err(|_error| CreateAclsAdmissionErrorKind::RetainedBytes)?;
        let owner_charge = request_owner_charge(
            &plan,
            prepared_results.capacity(),
            prepared_outcomes.capacity(),
        )
        .ok_or(CreateAclsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = CREATE_ACLS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(CreateAclsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(CREATE_ACLS_RETAINED_BYTES)
            .filter(|total| *total <= CREATE_ACLS_RETAINED_BYTES)
            .ok_or(CreateAclsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = CreateAclsOperation {
            operation_id,
            machine: CreateAclsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: CREATE_ACLS_RETAINED_BYTES,
            request_limit: remaining_result_bytes,
            result_limit: remaining_result_bytes,
            remaining_result_bytes,
            prepared_results: Some(prepared_results),
            prepared_outcomes: Some(prepared_outcomes),
            submission: None,
            handoff: CreateAclsHandoff::Untouched,
            call: None,
            recovered_call: None,
            raw_terminal: None,
            terminal: None,
            outcome: None,
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
        Ok(CreateAclsAdmission {
            observer: CreateAclsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut CreateAclsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, CreateAclsHostError> {
    let transition = operation.machine.apply(CreateAclsInput::Start { now })?;
    match transition.into_effect() {
        Some(CreateAclsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            route: kafka_client_core::CreateAclsRoute::AnyBroker,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(CreateAclsHostError::SubmissionMismatch);
            }
            operation.submission = Some(CreateAclsSubmission {
                operation_id,
                deadline,
                plan,
                request_limit: operation.remaining_result_bytes,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(CreateAclsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(CreateAclsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(CreateAclsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> CreateAclsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => CreateAclsAdmissionErrorKind::Capacity,
        _ => CreateAclsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(
    plan: &CreateAclsPlan,
    result_capacity: usize,
    outcome_capacity: usize,
) -> Option<usize> {
    let one_plan = plan_heap_charge(plan)?;
    size_of::<CreateAclsOperation>()
        .checked_add(size_of::<CreateAclsSubmission>())?
        .checked_add(one_plan.checked_mul(2)?)?
        .checked_add(result_capacity.checked_mul(size_of::<CreateAclResult>())?)?
        .checked_add(outcome_capacity.checked_mul(size_of::<CreateAclOutcome>())?)
}

fn plan_heap_charge(plan: &CreateAclsPlan) -> Option<usize> {
    plan.bindings().iter().try_fold(
        plan.bindings()
            .len()
            .checked_mul(size_of::<CreateAclBinding>())?,
        |total, binding| {
            total
                .checked_add(binding.resource_name().len())?
                .checked_add(binding.principal().len())?
                .checked_add(binding.host().len())
        },
    )
}
