//! Atomic completion and retained-byte reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    AdminDescribeTransactionOutcome, AdminDescribeTransactionsEffect,
    AdminDescribeTransactionsInput, AdminDescribeTransactionsMachine,
    AdminDescribeTransactionsPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ADMIN_DESCRIBE_TRANSACTIONS_CAPACITY, ADMIN_DESCRIBE_TRANSACTIONS_RETAINED_BYTES,
    AdminDescribeTransactionsAdmission, AdminDescribeTransactionsHandoff,
    AdminDescribeTransactionsHost, AdminDescribeTransactionsHostError,
    AdminDescribeTransactionsOperation,
};
use crate::admin::describe_transactions::{
    AdminDescribeTransactionsAdmissionErrorKind, AdminDescribeTransactionsObserver,
};

impl AdminDescribeTransactionsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminDescribeTransactionsPlan,
    ) -> Result<AdminDescribeTransactionsAdmission, AdminDescribeTransactionsAdmissionErrorKind>
    {
        if !self.accepting {
            return Err(AdminDescribeTransactionsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ADMIN_DESCRIBE_TRANSACTIONS_CAPACITY {
            return Err(AdminDescribeTransactionsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AdminDescribeTransactionsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(AdminDescribeTransactionsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = ADMIN_DESCRIBE_TRANSACTIONS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .ok_or(AdminDescribeTransactionsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(ADMIN_DESCRIBE_TRANSACTIONS_RETAINED_BYTES)
            .filter(|total| *total <= ADMIN_DESCRIBE_TRANSACTIONS_RETAINED_BYTES)
            .ok_or(AdminDescribeTransactionsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = AdminDescribeTransactionsOperation {
            operation_id,
            machine: AdminDescribeTransactionsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: ADMIN_DESCRIBE_TRANSACTIONS_RETAINED_BYTES,
            remaining_result_bytes,
            submission: None,
            handoff: AdminDescribeTransactionsHandoff::Untouched,
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
        Ok(AdminDescribeTransactionsAdmission {
            observer: AdminDescribeTransactionsObserver::from_completion(observer),
            fault,
        })
    }

    #[cfg(test)]
    pub(in crate::admin::describe_transactions) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}

fn start(
    operation: &mut AdminDescribeTransactionsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AdminDescribeTransactionsHostError> {
    let transition = operation
        .machine
        .apply(AdminDescribeTransactionsInput::Start { now })?;
    match transition.into_effect() {
        Some(AdminDescribeTransactionsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            transactional_id,
        }) if operation_id == operation.operation_id && core_deadline == deadline.core() => {
            operation.submission = Some(super::AdminDescribeTransactionsSubmission {
                operation_id,
                deadline,
                transactional_id,
            });
            Ok(false)
        }
        Some(AdminDescribeTransactionsEffect::Complete {
            operation_id,
            terminal,
        }) if operation_id == operation.operation_id => {
            operation.terminal = Some(terminal);
            Ok(true)
        }
        Some(_) => Err(AdminDescribeTransactionsHostError::SubmissionMismatch),
        None => Err(AdminDescribeTransactionsHostError::MissingSubmission),
    }
}

fn reservation_error(
    error: CompletionRegistryError,
) -> AdminDescribeTransactionsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AdminDescribeTransactionsAdmissionErrorKind::Capacity,
        _ => AdminDescribeTransactionsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AdminDescribeTransactionsPlan) -> Option<usize> {
    let id_bytes = plan
        .transactional_ids()
        .iter()
        .try_fold(0usize, |total, id| total.checked_add(id.len()))?;
    size_of::<AdminDescribeTransactionsOperation>()
        .checked_add(size_of::<super::AdminDescribeTransactionsSubmission>())?
        .checked_add(2usize.checked_mul(size_of::<AdminDescribeTransactionsPlan>())?)?
        .checked_add(
            plan.transactional_ids()
                .len()
                .checked_mul(size_of::<String>())?,
        )?
        .checked_add(
            plan.transactional_ids()
                .len()
                .checked_mul(size_of::<AdminDescribeTransactionOutcome>())?,
        )?
        .checked_add(2usize.checked_mul(id_bytes)?)
}
