//! Atomic completion and four-MiB retained-envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    AdminListTransactionsEffect, AdminListTransactionsInput, AdminListTransactionsMachine,
    AdminListTransactionsPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ADMIN_LIST_TRANSACTIONS_CAPACITY, ADMIN_LIST_TRANSACTIONS_RETAINED_BYTES,
    AdminListTransactionsAdmission, AdminListTransactionsHandoff, AdminListTransactionsHost,
    AdminListTransactionsHostError, AdminListTransactionsOperation,
    AdminListTransactionsSubmission, AdminListTransactionsSubmissionKind,
};
use crate::admin::list_transactions::{
    AdminListTransactionsAdmissionErrorKind, AdminListTransactionsObserver,
};

impl AdminListTransactionsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminListTransactionsPlan,
    ) -> Result<AdminListTransactionsAdmission, AdminListTransactionsAdmissionErrorKind> {
        if !self.accepting {
            return Err(AdminListTransactionsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ADMIN_LIST_TRANSACTIONS_CAPACITY {
            return Err(AdminListTransactionsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AdminListTransactionsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(AdminListTransactionsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = ADMIN_LIST_TRANSACTIONS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .ok_or(AdminListTransactionsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(ADMIN_LIST_TRANSACTIONS_RETAINED_BYTES)
            .filter(|total| *total <= ADMIN_LIST_TRANSACTIONS_RETAINED_BYTES)
            .ok_or(AdminListTransactionsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = AdminListTransactionsOperation {
            operation_id,
            machine: AdminListTransactionsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: ADMIN_LIST_TRANSACTIONS_RETAINED_BYTES,
            remaining_result_bytes,
            submission: None,
            active_submission: None,
            handoff: AdminListTransactionsHandoff::Untouched,
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
        Ok(AdminListTransactionsAdmission {
            observer: AdminListTransactionsObserver::from_completion(observer),
            fault,
        })
    }

    #[cfg(test)]
    pub(in crate::admin::list_transactions) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}

fn start(
    operation: &mut AdminListTransactionsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AdminListTransactionsHostError> {
    let transition = operation
        .machine
        .apply(AdminListTransactionsInput::Start { now })?;
    match transition.into_effect() {
        Some(AdminListTransactionsEffect::SubmitDiscovery {
            operation_id,
            deadline: core_deadline,
        }) if operation_id == operation.operation_id && core_deadline == deadline.core() => {
            operation.prepare_submission(AdminListTransactionsSubmissionKind::Discovery {
                retained_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(AdminListTransactionsEffect::Complete {
            operation_id,
            terminal,
        }) if operation_id == operation.operation_id => {
            operation.terminal = Some(terminal);
            Ok(true)
        }
        Some(_) => Err(AdminListTransactionsHostError::SubmissionMismatch),
        None => Err(AdminListTransactionsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> AdminListTransactionsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AdminListTransactionsAdmissionErrorKind::Capacity,
        _ => AdminListTransactionsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AdminListTransactionsPlan) -> Option<usize> {
    let state_bytes = plan
        .state_filters()
        .iter()
        .try_fold(0usize, |total, state| total.checked_add(state.len()))?;
    let state_owners = plan
        .state_filters()
        .len()
        .checked_mul(size_of::<String>())?;
    let producer_bytes = plan
        .producer_id_filters()
        .len()
        .checked_mul(size_of::<i64>())?;
    let pattern_bytes = plan.transactional_id_pattern().map_or(0, str::len);
    let plan_bytes = state_bytes
        .checked_add(state_owners)?
        .checked_add(producer_bytes)?
        .checked_add(pattern_bytes)?;
    size_of::<AdminListTransactionsOperation>()
        .checked_add(size_of::<AdminListTransactionsSubmission>())?
        .checked_add(3usize.checked_mul(size_of::<AdminListTransactionsPlan>())?)?
        .checked_add(3usize.checked_mul(plan_bytes)?)
}
