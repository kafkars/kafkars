//! Atomic completion and retained-byte reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    AdminDescribeLogDirsEffect, AdminDescribeLogDirsInput, AdminDescribeLogDirsMachine,
    AdminDescribeLogDirsPartition, AdminDescribeLogDirsPlan, Moment, OperationId,
};

use crate::protocol::admin::describe_log_dirs::selection_request_peak_charge;
use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DESCRIBE_LOG_DIRS_CAPACITY, DESCRIBE_LOG_DIRS_RETAINED_BYTES, DescribeLogDirsAdmission,
    DescribeLogDirsHandoff, DescribeLogDirsHost, DescribeLogDirsHostError,
    DescribeLogDirsOperation, DescribeLogDirsSubmission,
};
use crate::admin::describe_log_dirs::{DescribeLogDirsAdmissionErrorKind, DescribeLogDirsObserver};

impl DescribeLogDirsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminDescribeLogDirsPlan,
    ) -> Result<DescribeLogDirsAdmission, DescribeLogDirsAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeLogDirsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DESCRIBE_LOG_DIRS_CAPACITY {
            return Err(DescribeLogDirsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeLogDirsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge =
            request_owner_charge(&plan).ok_or(DescribeLogDirsAdmissionErrorKind::RetainedBytes)?;
        let request_scratch_limit = selection_request_peak_charge(plan.selection())
            .ok_or(DescribeLogDirsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = DESCRIBE_LOG_DIRS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .and_then(|limit| limit.checked_sub(request_scratch_limit))
            .filter(|limit| *limit > 0)
            .ok_or(DescribeLogDirsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(DESCRIBE_LOG_DIRS_RETAINED_BYTES)
            .filter(|total| *total <= DESCRIBE_LOG_DIRS_RETAINED_BYTES)
            .ok_or(DescribeLogDirsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let expected_plan = plan.clone();
        let mut operation = DescribeLogDirsOperation {
            operation_id,
            machine: AdminDescribeLogDirsMachine::new(operation_id, deadline.core(), plan),
            plan: expected_plan,
            completion_id,
            deadline,
            retained_bytes: DESCRIBE_LOG_DIRS_RETAINED_BYTES,
            request_scratch_limit,
            result_limit: remaining_result_bytes,
            remaining_result_bytes,
            submission: None,
            rejected_submission: None,
            handoff: DescribeLogDirsHandoff::Untouched,
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
        Ok(DescribeLogDirsAdmission {
            observer: DescribeLogDirsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeLogDirsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DescribeLogDirsHostError> {
    let transition = operation
        .machine
        .apply(AdminDescribeLogDirsInput::Start { now })?;
    match transition.into_effect() {
        Some(AdminDescribeLogDirsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            broker_id,
            selection,
        }) => {
            if operation_id != operation.operation_id
                || core_deadline != deadline.core()
                || operation.machine.current_broker() != Some(broker_id)
                || selection != *operation.plan.selection()
            {
                return Err(DescribeLogDirsHostError::SubmissionMismatch);
            }
            operation.submission = Some(DescribeLogDirsSubmission::new(
                operation_id,
                deadline,
                broker_id,
                selection,
                operation.request_scratch_limit,
                operation.result_limit,
            ));
            Ok(false)
        }
        Some(AdminDescribeLogDirsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DescribeLogDirsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeLogDirsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeLogDirsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeLogDirsAdmissionErrorKind::Capacity,
        _ => DescribeLogDirsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AdminDescribeLogDirsPlan) -> Option<usize> {
    let selected = plan.selection().selected_partitions().unwrap_or(&[]);
    let selection_structures = selected
        .len()
        .checked_mul(size_of::<AdminDescribeLogDirsPartition>().checked_mul(3)?)?;
    let topic_bytes = selected.iter().try_fold(0usize, |bytes, partition| {
        bytes.checked_add(partition.topic().len().checked_mul(3)?)
    })?;
    size_of::<DescribeLogDirsOperation>()
        .checked_add(size_of::<DescribeLogDirsSubmission>())?
        .checked_add(
            plan.broker_ids()
                .len()
                .checked_mul(size_of::<i32>().checked_mul(2)?)?,
        )?
        .checked_add(plan.broker_ids().len().checked_mul(size_of::<
            kafka_client_core::AdminDescribeLogDirsBrokerOutcome,
        >())?)?
        .checked_add(selection_structures)?
        .checked_add(topic_bytes)
}
