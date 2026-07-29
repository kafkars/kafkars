//! Atomic completion and one-MiB request-result envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    CreateDelegationTokenEffect, CreateDelegationTokenInput, CreateDelegationTokenMachine,
    CreateDelegationTokenPlan, DelegationTokenPrincipal, Moment, OperationId,
};

use crate::{
    clock::OperationDeadline, completion::CompletionRegistryError,
    protocol::admin::create_delegation_token::PreparedCreateDelegationTokenRequest,
};

use super::{
    CREATE_DELEGATION_TOKEN_CAPACITY, CREATE_DELEGATION_TOKEN_OPERATION_BYTES,
    CREATE_DELEGATION_TOKEN_RETAINED_BYTES, CreateDelegationTokenAdmission,
    CreateDelegationTokenHandoff, CreateDelegationTokenHost, CreateDelegationTokenHostError,
    CreateDelegationTokenOperation, CreateDelegationTokenSubmission,
};
use crate::admin::create_delegation_token::{
    CreateDelegationTokenAdmissionErrorKind, CreateDelegationTokenObserver,
};

impl CreateDelegationTokenHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: CreateDelegationTokenPlan,
        prepared_request: PreparedCreateDelegationTokenRequest,
    ) -> Result<CreateDelegationTokenAdmission, CreateDelegationTokenAdmissionErrorKind> {
        if !self.accepting {
            return Err(CreateDelegationTokenAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= CREATE_DELEGATION_TOKEN_CAPACITY {
            return Err(CreateDelegationTokenAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(CreateDelegationTokenAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan, &prepared_request)
            .ok_or(CreateDelegationTokenAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = CREATE_DELEGATION_TOKEN_OPERATION_BYTES
            .checked_sub(owner_charge)
            .filter(|remaining| *remaining > 0)
            .ok_or(CreateDelegationTokenAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(CREATE_DELEGATION_TOKEN_OPERATION_BYTES)
            .filter(|total| *total <= CREATE_DELEGATION_TOKEN_RETAINED_BYTES)
            .ok_or(CreateDelegationTokenAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = CreateDelegationTokenOperation {
            operation_id,
            machine: CreateDelegationTokenMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: CREATE_DELEGATION_TOKEN_OPERATION_BYTES,
            remaining_result_bytes,
            submission: None,
            handoff: CreateDelegationTokenHandoff::Untouched,
            call: None,
            recovered_call: None,
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
        Ok(CreateDelegationTokenAdmission {
            observer: CreateDelegationTokenObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut CreateDelegationTokenOperation,
    now: Moment,
    deadline: OperationDeadline,
    prepared_request: PreparedCreateDelegationTokenRequest,
) -> Result<bool, CreateDelegationTokenHostError> {
    let machine = &mut operation.machine;
    let transition = machine.apply(CreateDelegationTokenInput::Start { now })?;
    match transition.into_effect() {
        Some(CreateDelegationTokenEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(CreateDelegationTokenHostError::SubmissionMismatch);
            }
            operation.submission = Some(CreateDelegationTokenSubmission {
                operation_id,
                deadline,
                plan,
                prepared_request,
            });
            Ok(false)
        }
        Some(CreateDelegationTokenEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(CreateDelegationTokenHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(CreateDelegationTokenHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> CreateDelegationTokenAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => CreateDelegationTokenAdmissionErrorKind::Capacity,
        _ => CreateDelegationTokenAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(
    plan: &CreateDelegationTokenPlan,
    prepared: &PreparedCreateDelegationTokenRequest,
) -> Option<usize> {
    let principal_storage = plan
        .renewers()
        .len()
        .checked_add(usize::from(plan.owner().is_some()))?
        .checked_mul(size_of::<DelegationTokenPrincipal>())?;
    let text_bytes =
        plan.owner()
            .into_iter()
            .chain(plan.renewers())
            .try_fold(0usize, |bytes, principal| {
                bytes
                    .checked_add(principal.principal_type().len())?
                    .checked_add(principal.principal_name().len())
            })?;
    let one_plan = principal_storage.checked_add(text_bytes)?;
    size_of::<CreateDelegationTokenOperation>()
        .checked_add(size_of::<CreateDelegationTokenSubmission>())?
        .checked_add(one_plan.checked_mul(2)?)?
        .checked_add(prepared.retained_heap_bytes())
}
