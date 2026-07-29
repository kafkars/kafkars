//! Atomic completion and one-MiB secret request-result envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    EXPIRE_DELEGATION_TOKEN_MAX_HMAC_BYTES, ExpireDelegationTokenEffect,
    ExpireDelegationTokenInput, ExpireDelegationTokenMachine, ExpireDelegationTokenPlan, Moment,
    OperationId,
};

use crate::{
    clock::OperationDeadline, completion::CompletionRegistryError,
    protocol::admin::expire_delegation_token::PreparedExpireDelegationTokenRequest,
};

use super::{
    EXPIRE_DELEGATION_TOKEN_CAPACITY, EXPIRE_DELEGATION_TOKEN_OPERATION_BYTES,
    EXPIRE_DELEGATION_TOKEN_RETAINED_BYTES, ExpireDelegationTokenAdmission,
    ExpireDelegationTokenHandoff, ExpireDelegationTokenHost, ExpireDelegationTokenHostError,
    ExpireDelegationTokenOperation, ExpireDelegationTokenSubmission,
};
use crate::admin::expire_delegation_token::{
    ExpireDelegationTokenAdmissionErrorKind, ExpireDelegationTokenObserver,
};

impl ExpireDelegationTokenHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ExpireDelegationTokenPlan,
        prepared_request: PreparedExpireDelegationTokenRequest,
    ) -> Result<ExpireDelegationTokenAdmission, ExpireDelegationTokenAdmissionErrorKind> {
        if !self.accepting {
            return Err(ExpireDelegationTokenAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= EXPIRE_DELEGATION_TOKEN_CAPACITY {
            return Err(ExpireDelegationTokenAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(ExpireDelegationTokenAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan, &prepared_request)
            .ok_or(ExpireDelegationTokenAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = EXPIRE_DELEGATION_TOKEN_OPERATION_BYTES
            .checked_sub(owner_charge)
            .filter(|remaining| *remaining > 0)
            .ok_or(ExpireDelegationTokenAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(EXPIRE_DELEGATION_TOKEN_OPERATION_BYTES)
            .filter(|total| *total <= EXPIRE_DELEGATION_TOKEN_RETAINED_BYTES)
            .ok_or(ExpireDelegationTokenAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = ExpireDelegationTokenOperation {
            operation_id,
            machine: ExpireDelegationTokenMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: EXPIRE_DELEGATION_TOKEN_OPERATION_BYTES,
            remaining_result_bytes,
            submission: None,
            handoff: ExpireDelegationTokenHandoff::Untouched,
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
        Ok(ExpireDelegationTokenAdmission {
            observer: ExpireDelegationTokenObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut ExpireDelegationTokenOperation,
    now: Moment,
    deadline: OperationDeadline,
    prepared_request: PreparedExpireDelegationTokenRequest,
) -> Result<bool, ExpireDelegationTokenHostError> {
    let machine = &mut operation.machine;
    let transition = machine.apply(ExpireDelegationTokenInput::Start { now })?;
    match transition.into_effect() {
        Some(ExpireDelegationTokenEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(ExpireDelegationTokenHostError::SubmissionMismatch);
            }
            operation.submission = Some(ExpireDelegationTokenSubmission {
                operation_id,
                deadline,
                plan,
                prepared_request,
            });
            Ok(false)
        }
        Some(ExpireDelegationTokenEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(ExpireDelegationTokenHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(ExpireDelegationTokenHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> ExpireDelegationTokenAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => ExpireDelegationTokenAdmissionErrorKind::Capacity,
        _ => ExpireDelegationTokenAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(
    _plan: &ExpireDelegationTokenPlan,
    prepared: &PreparedExpireDelegationTokenRequest,
) -> Option<usize> {
    size_of::<ExpireDelegationTokenOperation>()
        .checked_add(size_of::<ExpireDelegationTokenSubmission>())?
        .checked_add(EXPIRE_DELEGATION_TOKEN_MAX_HMAC_BYTES)?
        .checked_add(prepared.retained_heap_bytes())
}
