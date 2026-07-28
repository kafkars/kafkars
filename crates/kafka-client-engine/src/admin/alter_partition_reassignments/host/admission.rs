//! Atomic completion and retained-envelope reservation for reassignment.

use core::mem::size_of;

use kafka_client_core::{
    AlterPartitionReassignment, AlterPartitionReassignmentsEffect,
    AlterPartitionReassignmentsInput, AlterPartitionReassignmentsMachine,
    AlterPartitionReassignmentsPlan, Moment, OperationId, PartitionReassignmentTarget,
};

use crate::{
    admin::alter_partition_reassignments::{
        AlterPartitionReassignmentsAdmissionErrorKind, AlterPartitionReassignmentsObserver,
    },
    clock::OperationDeadline,
    completion::CompletionRegistryError,
    protocol::admin::alter_partition_reassignments::{
        AlterPartitionReassignmentRef, generated_request_peak_charge,
    },
};

use super::{
    ALTER_PARTITION_REASSIGNMENTS_CAPACITY, ALTER_PARTITION_REASSIGNMENTS_RETAINED_BYTES,
    AlterPartitionReassignmentsAdmission, AlterPartitionReassignmentsHandoff,
    AlterPartitionReassignmentsHost, AlterPartitionReassignmentsOperation,
    AlterPartitionReassignmentsSubmission,
};

impl AlterPartitionReassignmentsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        AlterPartitionReassignmentsPlan,
        usize,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.request_scratch_limit,
            self.result_limit,
        )
    }
}

impl AlterPartitionReassignmentsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterPartitionReassignmentsPlan,
    ) -> Result<AlterPartitionReassignmentsAdmission, AlterPartitionReassignmentsAdmissionErrorKind>
    {
        if !self.accepting {
            return Err(AlterPartitionReassignmentsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ALTER_PARTITION_REASSIGNMENTS_CAPACITY {
            return Err(AlterPartitionReassignmentsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AlterPartitionReassignmentsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(AlterPartitionReassignmentsAdmissionErrorKind::RetainedBytes)?;
        let generated_request_charge =
            generated_request_peak_charge(plan.changes().iter().map(|change| {
                AlterPartitionReassignmentRef::new(
                    change.topic(),
                    change.partition(),
                    change.target().replicas(),
                )
            }))
            .ok_or(AlterPartitionReassignmentsAdmissionErrorKind::RetainedBytes)?;
        let result_limit = ALTER_PARTITION_REASSIGNMENTS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .and_then(|limit| limit.checked_sub(generated_request_charge))
            .filter(|limit| *limit > 0)
            .ok_or(AlterPartitionReassignmentsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(ALTER_PARTITION_REASSIGNMENTS_RETAINED_BYTES)
            .filter(|total| *total <= ALTER_PARTITION_REASSIGNMENTS_RETAINED_BYTES)
            .ok_or(AlterPartitionReassignmentsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let response_plan = plan.clone();
        let mut operation = AlterPartitionReassignmentsOperation {
            operation_id,
            machine: AlterPartitionReassignmentsMachine::new(operation_id, deadline.core(), plan),
            response_plan,
            completion_id,
            deadline,
            retained_bytes: ALTER_PARTITION_REASSIGNMENTS_RETAINED_BYTES,
            request_scratch_limit: generated_request_charge,
            result_limit,
            submission: None,
            handoff: AlterPartitionReassignmentsHandoff::Untouched,
            call: None,
            recovered_call: None,
            raw_terminal: None,
            terminal: None,
        };
        let transition = operation
            .machine
            .apply(AlterPartitionReassignmentsInput::Start { now })
            .map_err(|_error| AlterPartitionReassignmentsAdmissionErrorKind::HostUnavailable)?;
        match transition.into_effect() {
            Some(AlterPartitionReassignmentsEffect::Submit {
                operation_id: submitted_id,
                deadline: core_deadline,
                plan,
            }) if submitted_id == operation_id && core_deadline == deadline.core() => {
                operation.submission = Some(AlterPartitionReassignmentsSubmission {
                    operation_id,
                    deadline,
                    plan,
                    request_scratch_limit: generated_request_charge,
                    result_limit,
                });
            }
            Some(AlterPartitionReassignmentsEffect::Complete {
                operation_id: completed_id,
                terminal,
            }) if completed_id == operation_id => {
                operation.terminal = Some(terminal);
            }
            _ => return Err(AlterPartitionReassignmentsAdmissionErrorKind::HostUnavailable),
        }
        let terminal_ready = operation.terminal.is_some();
        self.operations.push(operation);
        let mut fault = None;
        if terminal_ready {
            if let Err(error) = self.publish_terminal(self.operations.len() - 1) {
                self.health = Some(error);
                fault = Some(error);
            }
        }
        Ok(AlterPartitionReassignmentsAdmission {
            observer: AlterPartitionReassignmentsObserver::from_completion(observer),
            fault,
        })
    }
}

fn reservation_error(
    error: CompletionRegistryError,
) -> AlterPartitionReassignmentsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AlterPartitionReassignmentsAdmissionErrorKind::Capacity,
        _ => AlterPartitionReassignmentsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AlterPartitionReassignmentsPlan) -> Option<usize> {
    let payload = plan.changes().iter().try_fold(0usize, |bytes, change| {
        bytes
            .checked_add(change.topic().len())?
            .checked_add(match change.target() {
                PartitionReassignmentTarget::Cancel => 0,
                PartitionReassignmentTarget::Replicas(replicas) => {
                    replicas.len().checked_mul(size_of::<i32>())?
                }
            })
    })?;
    size_of::<AlterPartitionReassignmentsOperation>()
        .checked_add(size_of::<AlterPartitionReassignmentsSubmission>())?
        .checked_add(3usize.checked_mul(size_of::<AlterPartitionReassignmentsPlan>())?)?
        .checked_add(
            3usize.checked_mul(
                plan.changes()
                    .len()
                    .checked_mul(size_of::<AlterPartitionReassignment>())?,
            )?,
        )?
        .checked_add(3usize.checked_mul(payload)?)
}
