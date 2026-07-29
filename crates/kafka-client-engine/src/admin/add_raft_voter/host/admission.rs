//! Atomic completion and voter-addition retained-envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    AddRaftVoterEffect, AddRaftVoterEndpoint, AddRaftVoterInput, AddRaftVoterMachine,
    AddRaftVoterPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ADD_RAFT_VOTER_CAPACITY, ADD_RAFT_VOTER_RESULT_BYTES, ADD_RAFT_VOTER_RETAINED_BYTES,
    AddRaftVoterAdmission, AddRaftVoterHandoff, AddRaftVoterHost, AddRaftVoterHostError,
    AddRaftVoterOperation, AddRaftVoterSubmission,
};
use crate::admin::add_raft_voter::{AddRaftVoterAdmissionErrorKind, AddRaftVoterObserver};

impl AddRaftVoterHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AddRaftVoterPlan,
    ) -> Result<AddRaftVoterAdmission, AddRaftVoterAdmissionErrorKind> {
        if !self.accepting {
            return Err(AddRaftVoterAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ADD_RAFT_VOTER_CAPACITY {
            return Err(AddRaftVoterAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AddRaftVoterAdmissionErrorKind::IdentityExhausted)?;
        let operation_bytes = request_owner_charge(&plan)
            .and_then(|charge| charge.checked_add(ADD_RAFT_VOTER_RESULT_BYTES))
            .ok_or(AddRaftVoterAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(operation_bytes)
            .filter(|total| *total <= ADD_RAFT_VOTER_RETAINED_BYTES)
            .ok_or(AddRaftVoterAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = ADD_RAFT_VOTER_RESULT_BYTES;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = AddRaftVoterOperation {
            operation_id,
            machine: AddRaftVoterMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: operation_bytes,
            remaining_result_bytes,
            submission: None,
            handoff: AddRaftVoterHandoff::Untouched,
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
        Ok(AddRaftVoterAdmission {
            observer: AddRaftVoterObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut AddRaftVoterOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AddRaftVoterHostError> {
    let transition = operation.machine.apply(AddRaftVoterInput::Start { now })?;
    match transition.into_effect() {
        Some(AddRaftVoterEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(AddRaftVoterHostError::SubmissionMismatch);
            }
            operation.submission = Some(AddRaftVoterSubmission {
                operation_id,
                deadline,
                plan,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(AddRaftVoterEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(AddRaftVoterHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(AddRaftVoterHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> AddRaftVoterAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AddRaftVoterAdmissionErrorKind::Capacity,
        _ => AddRaftVoterAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AddRaftVoterPlan) -> Option<usize> {
    let listener_storage = plan
        .listeners()
        .len()
        .checked_mul(size_of::<AddRaftVoterEndpoint>())?;
    let text_bytes = plan.listeners().iter().try_fold(
        plan.cluster_id().map_or(0, str::len),
        |bytes, endpoint| {
            bytes
                .checked_add(endpoint.name().len())
                .and_then(|bytes| bytes.checked_add(endpoint.host().len()))
        },
    )?;
    let one_plan = listener_storage.checked_add(text_bytes)?;
    size_of::<AddRaftVoterOperation>()
        .checked_add(size_of::<AddRaftVoterSubmission>())?
        .checked_add(one_plan.checked_mul(2)?)
}
