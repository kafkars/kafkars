//! Atomic completion and retained-byte reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    AbortPartitionTransactionEffect, AbortPartitionTransactionInput,
    AbortPartitionTransactionMachine, AbortPartitionTransactionPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ABORT_PARTITION_TRANSACTION_CAPACITY, ABORT_PARTITION_TRANSACTION_RETAINED_BYTES,
    AbortPartitionTransactionAdmission, AbortPartitionTransactionHandoff,
    AbortPartitionTransactionHost, AbortPartitionTransactionHostError,
    AbortPartitionTransactionOperation, AbortPartitionTransactionSubmission,
};
use crate::admin::abort_partition_transaction::{
    AbortPartitionTransactionAdmissionErrorKind, AbortPartitionTransactionObserver,
};

impl AbortPartitionTransactionHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AbortPartitionTransactionPlan,
    ) -> Result<AbortPartitionTransactionAdmission, AbortPartitionTransactionAdmissionErrorKind>
    {
        if !self.accepting {
            return Err(AbortPartitionTransactionAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ABORT_PARTITION_TRANSACTION_CAPACITY {
            return Err(AbortPartitionTransactionAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AbortPartitionTransactionAdmissionErrorKind::IdentityExhausted)?;
        let operation_bytes = request_owner_charge(&plan)
            .ok_or(AbortPartitionTransactionAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(operation_bytes)
            .filter(|total| *total <= ABORT_PARTITION_TRANSACTION_RETAINED_BYTES)
            .ok_or(AbortPartitionTransactionAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let retained_plan = plan.clone();
        let mut operation = AbortPartitionTransactionOperation {
            operation_id,
            machine: AbortPartitionTransactionMachine::new(operation_id, deadline.core(), plan),
            plan: retained_plan,
            completion_id,
            deadline,
            retained_bytes: operation_bytes,
            submission: None,
            handoff: AbortPartitionTransactionHandoff::Untouched,
            call: None,
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
        if terminal_ready && let Err(error) = self.publish_terminal(self.operations.len() - 1) {
            self.health = Some(error);
            fault = Some(error);
        }
        Ok(AbortPartitionTransactionAdmission {
            observer: AbortPartitionTransactionObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut AbortPartitionTransactionOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AbortPartitionTransactionHostError> {
    let transition = operation
        .machine
        .apply(AbortPartitionTransactionInput::Start { now })?;
    match transition.into_effect() {
        Some(AbortPartitionTransactionEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(AbortPartitionTransactionHostError::SubmissionMismatch);
            }
            operation.submission = Some(AbortPartitionTransactionSubmission {
                operation_id,
                deadline,
                plan,
            });
            Ok(false)
        }
        Some(AbortPartitionTransactionEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(AbortPartitionTransactionHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(AbortPartitionTransactionHostError::MissingSubmission),
    }
}

fn reservation_error(
    error: CompletionRegistryError,
) -> AbortPartitionTransactionAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AbortPartitionTransactionAdmissionErrorKind::Capacity,
        _ => AbortPartitionTransactionAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AbortPartitionTransactionPlan) -> Option<usize> {
    size_of::<AbortPartitionTransactionOperation>()
        .checked_add(size_of::<AbortPartitionTransactionSubmission>())?
        .checked_add(3usize.checked_mul(size_of::<AbortPartitionTransactionPlan>())?)?
        .checked_add(3usize.checked_mul(plan.topic().len())?)
}
