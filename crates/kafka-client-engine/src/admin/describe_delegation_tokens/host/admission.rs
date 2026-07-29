//! Atomic completion and four-MiB request-result envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    DelegationTokenPrincipal, DescribeDelegationTokensEffect, DescribeDelegationTokensInput,
    DescribeDelegationTokensMachine, DescribeDelegationTokensPlan,
    DescribeDelegationTokensSelection, Moment, OperationId,
};

use crate::{
    clock::OperationDeadline, completion::CompletionRegistryError,
    protocol::admin::describe_delegation_tokens::PreparedDescribeDelegationTokensRequest,
};

use super::{
    DESCRIBE_DELEGATION_TOKENS_CAPACITY, DESCRIBE_DELEGATION_TOKENS_OPERATION_BYTES,
    DESCRIBE_DELEGATION_TOKENS_RETAINED_BYTES, DescribeDelegationTokensAdmission,
    DescribeDelegationTokensHandoff, DescribeDelegationTokensHost,
    DescribeDelegationTokensHostError, DescribeDelegationTokensOperation,
    DescribeDelegationTokensSubmission,
};
use crate::admin::describe_delegation_tokens::{
    DescribeDelegationTokensAdmissionErrorKind, DescribeDelegationTokensObserver,
};

impl DescribeDelegationTokensHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeDelegationTokensPlan,
        prepared_request: PreparedDescribeDelegationTokensRequest,
    ) -> Result<DescribeDelegationTokensAdmission, DescribeDelegationTokensAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeDelegationTokensAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DESCRIBE_DELEGATION_TOKENS_CAPACITY {
            return Err(DescribeDelegationTokensAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeDelegationTokensAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan, &prepared_request)
            .ok_or(DescribeDelegationTokensAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = DESCRIBE_DELEGATION_TOKENS_OPERATION_BYTES
            .checked_sub(owner_charge)
            .filter(|remaining| *remaining > 0)
            .ok_or(DescribeDelegationTokensAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(DESCRIBE_DELEGATION_TOKENS_OPERATION_BYTES)
            .filter(|total| *total <= DESCRIBE_DELEGATION_TOKENS_RETAINED_BYTES)
            .ok_or(DescribeDelegationTokensAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = DescribeDelegationTokensOperation {
            operation_id,
            correlation_plan: plan.clone(),
            machine: DescribeDelegationTokensMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: DESCRIBE_DELEGATION_TOKENS_OPERATION_BYTES,
            remaining_result_bytes,
            submission: None,
            handoff: DescribeDelegationTokensHandoff::Untouched,
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
        Ok(DescribeDelegationTokensAdmission {
            observer: DescribeDelegationTokensObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeDelegationTokensOperation,
    now: Moment,
    deadline: OperationDeadline,
    prepared_request: PreparedDescribeDelegationTokensRequest,
) -> Result<bool, DescribeDelegationTokensHostError> {
    let machine = &mut operation.machine;
    let transition = machine.apply(DescribeDelegationTokensInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeDelegationTokensEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(DescribeDelegationTokensHostError::SubmissionMismatch);
            }
            operation.submission = Some(DescribeDelegationTokensSubmission {
                operation_id,
                deadline,
                plan,
                prepared_request,
            });
            Ok(false)
        }
        Some(DescribeDelegationTokensEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DescribeDelegationTokensHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeDelegationTokensHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeDelegationTokensAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeDelegationTokensAdmissionErrorKind::Capacity,
        _ => DescribeDelegationTokensAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(
    plan: &DescribeDelegationTokensPlan,
    prepared: &PreparedDescribeDelegationTokensRequest,
) -> Option<usize> {
    let (principal_storage, text_bytes) = match plan.selection() {
        DescribeDelegationTokensSelection::All => (0, 0),
        DescribeDelegationTokensSelection::Owners(owners) => {
            let storage = owners
                .len()
                .checked_mul(size_of::<DelegationTokenPrincipal>())?;
            let text = owners.iter().try_fold(0usize, |bytes, owner| {
                bytes
                    .checked_add(owner.principal_type().len())?
                    .checked_add(owner.principal_name().len())
            })?;
            (storage, text)
        }
    };
    let one_plan = principal_storage.checked_add(text_bytes)?;
    size_of::<DescribeDelegationTokensOperation>()
        .checked_add(size_of::<DescribeDelegationTokensSubmission>())?
        .checked_add(one_plan.checked_mul(3)?)?
        .checked_add(prepared.retained_heap_bytes())
}
