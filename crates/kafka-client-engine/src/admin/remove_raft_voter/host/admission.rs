//! Atomic completion and voter-removal envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    Moment, OperationId, RemoveRaftVoterEffect, RemoveRaftVoterInput, RemoveRaftVoterMachine,
    RemoveRaftVoterPlan,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    REMOVE_RAFT_VOTER_CAPACITY, REMOVE_RAFT_VOTER_RESULT_BYTES, REMOVE_RAFT_VOTER_RETAINED_BYTES,
    RemoveRaftVoterAdmission, RemoveRaftVoterHandoff, RemoveRaftVoterHost,
    RemoveRaftVoterHostError, RemoveRaftVoterOperation, RemoveRaftVoterSubmission,
};
use crate::admin::remove_raft_voter::{RemoveRaftVoterAdmissionErrorKind, RemoveRaftVoterObserver};

impl RemoveRaftVoterHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: RemoveRaftVoterPlan,
    ) -> Result<RemoveRaftVoterAdmission, RemoveRaftVoterAdmissionErrorKind> {
        if !self.accepting {
            return Err(RemoveRaftVoterAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= REMOVE_RAFT_VOTER_CAPACITY {
            return Err(RemoveRaftVoterAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(RemoveRaftVoterAdmissionErrorKind::IdentityExhausted)?;
        let cluster_bytes = plan.cluster_id().map_or(0, str::len);
        let operation_bytes = request_owner_charge(cluster_bytes)
            .and_then(|charge| charge.checked_add(REMOVE_RAFT_VOTER_RESULT_BYTES))
            .ok_or(RemoveRaftVoterAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(operation_bytes)
            .filter(|total| *total <= REMOVE_RAFT_VOTER_RETAINED_BYTES)
            .ok_or(RemoveRaftVoterAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = RemoveRaftVoterOperation {
            operation_id,
            machine: RemoveRaftVoterMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: operation_bytes,
            remaining_result_bytes: REMOVE_RAFT_VOTER_RESULT_BYTES,
            submission: None,
            handoff: RemoveRaftVoterHandoff::Untouched,
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
        if terminal_ready && let Err(error) = self.publish_terminal(self.operations.len() - 1) {
            self.health = Some(error);
            fault = Some(error);
        }
        Ok(RemoveRaftVoterAdmission {
            observer: RemoveRaftVoterObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut RemoveRaftVoterOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, RemoveRaftVoterHostError> {
    let transition = operation
        .machine
        .apply(RemoveRaftVoterInput::Start { now })?;
    match transition.into_effect() {
        Some(RemoveRaftVoterEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(RemoveRaftVoterHostError::SubmissionMismatch);
            }
            operation.submission = Some(RemoveRaftVoterSubmission {
                operation_id,
                deadline,
                plan,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(RemoveRaftVoterEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(RemoveRaftVoterHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(RemoveRaftVoterHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> RemoveRaftVoterAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => RemoveRaftVoterAdmissionErrorKind::Capacity,
        _ => RemoveRaftVoterAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(cluster_bytes: usize) -> Option<usize> {
    size_of::<RemoveRaftVoterOperation>()
        .checked_add(size_of::<RemoveRaftVoterSubmission>())?
        .checked_add(cluster_bytes.checked_mul(2)?)
}
