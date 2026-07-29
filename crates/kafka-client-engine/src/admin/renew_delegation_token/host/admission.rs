//! Atomic completion and one-MiB secret request-result envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    Moment, OperationId, RENEW_DELEGATION_TOKEN_MAX_HMAC_BYTES, RenewDelegationTokenEffect,
    RenewDelegationTokenInput, RenewDelegationTokenMachine, RenewDelegationTokenPlan,
};

use crate::{
    clock::OperationDeadline, completion::CompletionRegistryError,
    protocol::admin::renew_delegation_token::PreparedRenewDelegationTokenRequest,
};

use super::{
    RENEW_DELEGATION_TOKEN_CAPACITY, RENEW_DELEGATION_TOKEN_OPERATION_BYTES,
    RENEW_DELEGATION_TOKEN_RETAINED_BYTES, RenewDelegationTokenAdmission,
    RenewDelegationTokenHandoff, RenewDelegationTokenHost, RenewDelegationTokenHostError,
    RenewDelegationTokenOperation, RenewDelegationTokenSubmission,
};
use crate::admin::renew_delegation_token::{
    RenewDelegationTokenAdmissionErrorKind, RenewDelegationTokenObserver,
};

impl RenewDelegationTokenHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: RenewDelegationTokenPlan,
        prepared_request: PreparedRenewDelegationTokenRequest,
    ) -> Result<RenewDelegationTokenAdmission, RenewDelegationTokenAdmissionErrorKind> {
        if !self.accepting {
            return Err(RenewDelegationTokenAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= RENEW_DELEGATION_TOKEN_CAPACITY {
            return Err(RenewDelegationTokenAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(RenewDelegationTokenAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan, &prepared_request)
            .ok_or(RenewDelegationTokenAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = RENEW_DELEGATION_TOKEN_OPERATION_BYTES
            .checked_sub(owner_charge)
            .filter(|remaining| *remaining > 0)
            .ok_or(RenewDelegationTokenAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(RENEW_DELEGATION_TOKEN_OPERATION_BYTES)
            .filter(|total| *total <= RENEW_DELEGATION_TOKEN_RETAINED_BYTES)
            .ok_or(RenewDelegationTokenAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = RenewDelegationTokenOperation {
            operation_id,
            machine: RenewDelegationTokenMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: RENEW_DELEGATION_TOKEN_OPERATION_BYTES,
            remaining_result_bytes,
            submission: None,
            handoff: RenewDelegationTokenHandoff::Untouched,
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
        Ok(RenewDelegationTokenAdmission {
            observer: RenewDelegationTokenObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut RenewDelegationTokenOperation,
    now: Moment,
    deadline: OperationDeadline,
    prepared_request: PreparedRenewDelegationTokenRequest,
) -> Result<bool, RenewDelegationTokenHostError> {
    let machine = &mut operation.machine;
    let transition = machine.apply(RenewDelegationTokenInput::Start { now })?;
    match transition.into_effect() {
        Some(RenewDelegationTokenEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(RenewDelegationTokenHostError::SubmissionMismatch);
            }
            operation.submission = Some(RenewDelegationTokenSubmission {
                operation_id,
                deadline,
                plan,
                prepared_request,
            });
            Ok(false)
        }
        Some(RenewDelegationTokenEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(RenewDelegationTokenHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(RenewDelegationTokenHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> RenewDelegationTokenAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => RenewDelegationTokenAdmissionErrorKind::Capacity,
        _ => RenewDelegationTokenAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(
    _plan: &RenewDelegationTokenPlan,
    prepared: &PreparedRenewDelegationTokenRequest,
) -> Option<usize> {
    size_of::<RenewDelegationTokenOperation>()
        .checked_add(size_of::<RenewDelegationTokenSubmission>())?
        .checked_add(RENEW_DELEGATION_TOKEN_MAX_HMAC_BYTES)?
        .checked_add(prepared.retained_heap_bytes())
}
