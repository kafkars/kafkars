//! Atomic completion and retained-byte reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    AlterReplicaLogDirAssignment, AlterReplicaLogDirOutcome, AlterReplicaLogDirsEffect,
    AlterReplicaLogDirsInput, AlterReplicaLogDirsMachine, AlterReplicaLogDirsPlan, Moment,
    OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ALTER_REPLICA_LOG_DIRS_CAPACITY, ALTER_REPLICA_LOG_DIRS_RETAINED_BYTES,
    AlterReplicaLogDirsAdmission, AlterReplicaLogDirsAttempt, AlterReplicaLogDirsAttemptBounds,
    AlterReplicaLogDirsHandoff, AlterReplicaLogDirsHost, AlterReplicaLogDirsHostError,
    AlterReplicaLogDirsOperation, AlterReplicaLogDirsSubmission,
};
use crate::admin::alter_replica_log_dirs::{
    AlterReplicaLogDirsAdmissionErrorKind, AlterReplicaLogDirsObserver,
};

impl AlterReplicaLogDirsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterReplicaLogDirsPlan,
    ) -> Result<AlterReplicaLogDirsAdmission, AlterReplicaLogDirsAdmissionErrorKind> {
        if !self.accepting {
            return Err(AlterReplicaLogDirsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ALTER_REPLICA_LOG_DIRS_CAPACITY {
            return Err(AlterReplicaLogDirsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AlterReplicaLogDirsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(AlterReplicaLogDirsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = ALTER_REPLICA_LOG_DIRS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .ok_or(AlterReplicaLogDirsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(ALTER_REPLICA_LOG_DIRS_RETAINED_BYTES)
            .filter(|total| *total <= ALTER_REPLICA_LOG_DIRS_RETAINED_BYTES)
            .ok_or(AlterReplicaLogDirsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = AlterReplicaLogDirsOperation {
            operation_id,
            machine: AlterReplicaLogDirsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: ALTER_REPLICA_LOG_DIRS_RETAINED_BYTES,
            remaining_result_bytes,
            submission: None,
            attempt: None,
            handoff: AlterReplicaLogDirsHandoff::Untouched,
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
        Ok(AlterReplicaLogDirsAdmission {
            observer: AlterReplicaLogDirsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut AlterReplicaLogDirsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AlterReplicaLogDirsHostError> {
    let transition = operation
        .machine
        .apply(AlterReplicaLogDirsInput::Start { now })?;
    match transition.into_effect() {
        Some(AlterReplicaLogDirsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            broker_id,
            assignments,
        }) => {
            if operation_id != operation.operation_id
                || core_deadline != deadline.core()
                || operation.machine.current_broker() != Some(broker_id)
                || assignments.is_empty()
                || assignments
                    .iter()
                    .any(|assignment| assignment.broker_id() != broker_id)
            {
                return Err(AlterReplicaLogDirsHostError::SubmissionMismatch);
            }
            let bounds = AlterReplicaLogDirsAttemptBounds {
                request_scratch_limit: operation.remaining_result_bytes,
                result_limit: operation.remaining_result_bytes,
            };
            operation.attempt = Some(AlterReplicaLogDirsAttempt {
                broker_id,
                assignments: assignments.clone(),
                bounds,
            });
            operation.submission = Some(AlterReplicaLogDirsSubmission {
                operation_id,
                deadline,
                broker_id,
                assignments,
                bounds,
            });
            Ok(false)
        }
        Some(AlterReplicaLogDirsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(AlterReplicaLogDirsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(AlterReplicaLogDirsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> AlterReplicaLogDirsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AlterReplicaLogDirsAdmissionErrorKind::Capacity,
        _ => AlterReplicaLogDirsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AlterReplicaLogDirsPlan) -> Option<usize> {
    let mut charge = size_of::<AlterReplicaLogDirsOperation>()
        .checked_add(size_of::<AlterReplicaLogDirsSubmission>())?
        .checked_add(plan.broker_ids().len().checked_mul(size_of::<i32>())?)?
        .checked_add(
            plan.assignments()
                .len()
                .checked_mul(size_of::<AlterReplicaLogDirAssignment>().checked_mul(3)?)?,
        )?
        .checked_add(
            plan.assignments()
                .len()
                .checked_mul(size_of::<AlterReplicaLogDirOutcome>())?,
        )?;
    for assignment in plan.assignments() {
        let string_bytes = assignment
            .topic()
            .len()
            .checked_add(assignment.log_dir().len())?;
        charge = charge.checked_add(string_bytes.checked_mul(4)?)?;
    }
    Some(charge)
}
