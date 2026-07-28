//! Atomic completion and four-MiB envelope reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    ListPartitionReassignmentTarget, ListPartitionReassignmentsEffect,
    ListPartitionReassignmentsInput, ListPartitionReassignmentsMachine,
    ListPartitionReassignmentsPlan, ListPartitionReassignmentsSelection, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    LIST_PARTITION_REASSIGNMENTS_CAPACITY, LIST_PARTITION_REASSIGNMENTS_RETAINED_BYTES,
    ListPartitionReassignmentsAdmission, ListPartitionReassignmentsHandoff,
    ListPartitionReassignmentsHost, ListPartitionReassignmentsHostError,
    ListPartitionReassignmentsOperation, ListPartitionReassignmentsSubmission,
};
use crate::admin::list_partition_reassignments::{
    ListPartitionReassignmentsAdmissionErrorKind, ListPartitionReassignmentsObserver,
};

impl ListPartitionReassignmentsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ListPartitionReassignmentsPlan,
    ) -> Result<ListPartitionReassignmentsAdmission, ListPartitionReassignmentsAdmissionErrorKind>
    {
        if !self.accepting {
            return Err(ListPartitionReassignmentsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= LIST_PARTITION_REASSIGNMENTS_CAPACITY {
            return Err(ListPartitionReassignmentsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(ListPartitionReassignmentsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(ListPartitionReassignmentsAdmissionErrorKind::RetainedBytes)?;
        let result_limit = LIST_PARTITION_REASSIGNMENTS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(ListPartitionReassignmentsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(LIST_PARTITION_REASSIGNMENTS_RETAINED_BYTES)
            .filter(|total| *total <= LIST_PARTITION_REASSIGNMENTS_RETAINED_BYTES)
            .ok_or(ListPartitionReassignmentsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let host_plan = plan.clone();
        let mut operation = ListPartitionReassignmentsOperation {
            operation_id,
            machine: ListPartitionReassignmentsMachine::new(operation_id, deadline.core(), plan),
            plan: host_plan,
            completion_id,
            deadline,
            retained_bytes: LIST_PARTITION_REASSIGNMENTS_RETAINED_BYTES,
            result_limit,
            submission: None,
            rejected_submission: None,
            handoff: ListPartitionReassignmentsHandoff::Untouched,
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
        Ok(ListPartitionReassignmentsAdmission {
            observer: ListPartitionReassignmentsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut ListPartitionReassignmentsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, ListPartitionReassignmentsHostError> {
    let transition = operation
        .machine
        .apply(ListPartitionReassignmentsInput::Start { now })?;
    match transition.into_effect() {
        Some(ListPartitionReassignmentsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(ListPartitionReassignmentsHostError::SubmissionMismatch);
            }
            operation.submission = Some(ListPartitionReassignmentsSubmission {
                operation_id,
                deadline,
                plan,
                result_limit: operation.result_limit,
            });
            Ok(false)
        }
        Some(ListPartitionReassignmentsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(ListPartitionReassignmentsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(ListPartitionReassignmentsHostError::MissingSubmission),
    }
}

fn reservation_error(
    error: CompletionRegistryError,
) -> ListPartitionReassignmentsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => ListPartitionReassignmentsAdmissionErrorKind::Capacity,
        _ => ListPartitionReassignmentsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &ListPartitionReassignmentsPlan) -> Option<usize> {
    let (target_count, topic_bytes) = match plan.selection() {
        ListPartitionReassignmentsSelection::Selected(targets) => (
            targets.len(),
            targets.iter().try_fold(0usize, |bytes, target| {
                bytes.checked_add(target.topic().len())
            })?,
        ),
        ListPartitionReassignmentsSelection::AllActive => (0, 0),
    };
    size_of::<ListPartitionReassignmentsOperation>()
        .checked_add(size_of::<ListPartitionReassignmentsSubmission>())?
        .checked_add(3usize.checked_mul(size_of::<ListPartitionReassignmentsPlan>())?)?
        .checked_add(3usize.checked_mul(
            target_count.checked_mul(size_of::<ListPartitionReassignmentTarget>())?,
        )?)?
        .checked_add(3usize.checked_mul(topic_bytes)?)
}
