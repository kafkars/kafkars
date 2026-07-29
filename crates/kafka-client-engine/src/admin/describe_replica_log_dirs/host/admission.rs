//! Atomic completion and retained-byte reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    DescribeReplicaLogDirsEffect, DescribeReplicaLogDirsInput, DescribeReplicaLogDirsMachine,
    DescribeReplicaLogDirsPlan, DescribeReplicaLogDirsReplica,
    DescribeReplicaLogDirsReplicaOutcome, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DESCRIBE_REPLICA_LOG_DIRS_CAPACITY, DESCRIBE_REPLICA_LOG_DIRS_RETAINED_BYTES,
    DescribeReplicaLogDirsAdmission, DescribeReplicaLogDirsHandoff, DescribeReplicaLogDirsHost,
    DescribeReplicaLogDirsHostError, DescribeReplicaLogDirsOperation,
    DescribeReplicaLogDirsSubmission,
};
use crate::admin::describe_replica_log_dirs::{
    DescribeReplicaLogDirsAdmissionErrorKind, DescribeReplicaLogDirsObserver,
};

impl DescribeReplicaLogDirsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeReplicaLogDirsPlan,
    ) -> Result<DescribeReplicaLogDirsAdmission, DescribeReplicaLogDirsAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeReplicaLogDirsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DESCRIBE_REPLICA_LOG_DIRS_CAPACITY {
            return Err(DescribeReplicaLogDirsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeReplicaLogDirsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(DescribeReplicaLogDirsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = DESCRIBE_REPLICA_LOG_DIRS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .ok_or(DescribeReplicaLogDirsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(DESCRIBE_REPLICA_LOG_DIRS_RETAINED_BYTES)
            .filter(|total| *total <= DESCRIBE_REPLICA_LOG_DIRS_RETAINED_BYTES)
            .ok_or(DescribeReplicaLogDirsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = DescribeReplicaLogDirsOperation {
            operation_id,
            machine: DescribeReplicaLogDirsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: DESCRIBE_REPLICA_LOG_DIRS_RETAINED_BYTES,
            remaining_result_bytes,
            submission: None,
            current_replicas: None,
            handoff: DescribeReplicaLogDirsHandoff::Untouched,
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
        Ok(DescribeReplicaLogDirsAdmission {
            observer: DescribeReplicaLogDirsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeReplicaLogDirsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DescribeReplicaLogDirsHostError> {
    let transition = operation
        .machine
        .apply(DescribeReplicaLogDirsInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeReplicaLogDirsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            broker_id,
            replicas,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(DescribeReplicaLogDirsHostError::SubmissionMismatch);
            }
            operation.submission = Some(DescribeReplicaLogDirsSubmission {
                operation_id,
                deadline,
                broker_id,
                replicas,
                request_retained_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(DescribeReplicaLogDirsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DescribeReplicaLogDirsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeReplicaLogDirsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeReplicaLogDirsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeReplicaLogDirsAdmissionErrorKind::Capacity,
        _ => DescribeReplicaLogDirsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &DescribeReplicaLogDirsPlan) -> Option<usize> {
    let replica_structures = plan
        .replicas()
        .len()
        .checked_mul(size_of::<DescribeReplicaLogDirsReplica>().checked_mul(4)?)?;
    let outcome_structures = plan
        .replicas()
        .len()
        .checked_mul(size_of::<Option<DescribeReplicaLogDirsReplicaOutcome>>())?;
    let topic_bytes = plan.replicas().iter().try_fold(0usize, |bytes, replica| {
        bytes.checked_add(replica.topic().len().checked_mul(4)?)
    })?;
    size_of::<DescribeReplicaLogDirsOperation>()
        .checked_add(size_of::<DescribeReplicaLogDirsSubmission>())?
        .checked_add(plan.broker_ids().len().checked_mul(size_of::<i32>())?)?
        .checked_add(replica_structures)?
        .checked_add(outcome_structures)?
        .checked_add(topic_bytes)
}
