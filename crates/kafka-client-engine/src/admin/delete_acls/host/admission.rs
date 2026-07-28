//! Atomic terminal, positional-storage, and eight-MiB envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    DeleteAclFilterResult, DeleteAclsEffect, DeleteAclsFilter, DeleteAclsInput, DeleteAclsMachine,
    DeleteAclsPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DELETE_ACLS_CAPACITY, DELETE_ACLS_RETAINED_BYTES, DeleteAclsAdmission, DeleteAclsHandoff,
    DeleteAclsHost, DeleteAclsHostError, DeleteAclsOperation, DeleteAclsSubmission,
    model::DeleteAclsAttemptBounds,
};
use crate::admin::delete_acls::{
    DeleteAclsAdmissionErrorKind, DeleteAclsBatch, DeleteAclsObserver,
};

impl DeleteAclsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteAclsPlan,
    ) -> Result<DeleteAclsAdmission, DeleteAclsAdmissionErrorKind> {
        if !self.accepting {
            return Err(DeleteAclsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DELETE_ACLS_CAPACITY {
            return Err(DeleteAclsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DeleteAclsAdmissionErrorKind::IdentityExhausted)?;
        let filter_count = plan.required_filter_result_capacity();
        let mut prepared_results = Vec::new();
        prepared_results
            .try_reserve_exact(filter_count)
            .map_err(|_error| DeleteAclsAdmissionErrorKind::RetainedBytes)?;
        let prepared_core_result_bytes =
            vector_bytes::<DeleteAclFilterResult>(prepared_results.capacity())
                .ok_or(DeleteAclsAdmissionErrorKind::RetainedBytes)?;
        let mut matching_counts = Vec::new();
        matching_counts
            .try_reserve_exact(filter_count)
            .map_err(|_error| DeleteAclsAdmissionErrorKind::RetainedBytes)?;
        let prepared_outcomes = DeleteAclsBatch::try_prepare_outcomes(filter_count)
            .map_err(|_error| DeleteAclsAdmissionErrorKind::RetainedBytes)?;
        let prepared_outcome_bytes = prepared_outcomes
            .retained_heap_bytes()
            .ok_or(DeleteAclsAdmissionErrorKind::RetainedBytes)?;
        let result_capacity = prepared_results.capacity();
        let nested_count_capacity = matching_counts.capacity();
        let outcome_capacity = prepared_outcomes.outcomes_capacity();
        let owner_charge = request_owner_charge(
            &plan,
            prepared_core_result_bytes,
            matching_counts.capacity(),
            prepared_outcome_bytes,
        )
        .ok_or(DeleteAclsAdmissionErrorKind::RetainedBytes)?;
        let remaining_response_bytes = DELETE_ACLS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(DeleteAclsAdmissionErrorKind::RetainedBytes)?;
        let bounds = DeleteAclsAttemptBounds {
            request_limit: remaining_response_bytes,
            nested_count_capacity,
            result_capacity,
            outcome_capacity,
        };
        let total_bytes = self
            .retained_bytes
            .checked_add(DELETE_ACLS_RETAINED_BYTES)
            .filter(|total| *total <= DELETE_ACLS_RETAINED_BYTES)
            .ok_or(DeleteAclsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = DeleteAclsOperation {
            operation_id,
            machine: DeleteAclsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: DELETE_ACLS_RETAINED_BYTES,
            remaining_response_bytes,
            prepared_core_result_bytes,
            prepared_results: Some(prepared_results),
            matching_counts,
            prepared_outcomes: Some(prepared_outcomes),
            prepared_outcome_bytes,
            bounds,
            submission: None,
            handoff: DeleteAclsHandoff::Untouched,
            call: None,
            recovered_call: None,
            raw_terminal: None,
            terminal: None,
            outcome: None,
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
        Ok(DeleteAclsAdmission {
            observer: DeleteAclsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DeleteAclsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DeleteAclsHostError> {
    let transition = operation.machine.apply(DeleteAclsInput::Start { now })?;
    match transition.into_effect() {
        Some(DeleteAclsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            route: kafka_client_core::DeleteAclsRoute::AnyBroker,
            plan,
        }) => {
            if operation_id != operation.operation_id
                || core_deadline != deadline.core()
                || operation.machine.plan() != Some(&plan)
            {
                return Err(DeleteAclsHostError::SubmissionMismatch);
            }
            operation.submission = Some(DeleteAclsSubmission {
                operation_id,
                deadline,
                plan,
                bounds: operation.bounds,
            });
            Ok(false)
        }
        Some(DeleteAclsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DeleteAclsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DeleteAclsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DeleteAclsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DeleteAclsAdmissionErrorKind::Capacity,
        _ => DeleteAclsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(
    plan: &DeleteAclsPlan,
    prepared_core_result_bytes: usize,
    matching_count_capacity: usize,
    prepared_outcome_bytes: usize,
) -> Option<usize> {
    let one_plan = plan_heap_charge(plan)?;
    size_of::<DeleteAclsOperation>()
        .checked_add(size_of::<DeleteAclsSubmission>())?
        .checked_add(one_plan.checked_mul(2)?)?
        .checked_add(prepared_core_result_bytes)?
        .checked_add(vector_bytes::<usize>(matching_count_capacity)?)?
        .checked_add(prepared_outcome_bytes)
}

fn plan_heap_charge(plan: &DeleteAclsPlan) -> Option<usize> {
    plan.filters().iter().try_fold(
        plan.filters()
            .len()
            .checked_mul(size_of::<DeleteAclsFilter>())?,
        |total, filter| {
            total
                .checked_add(filter.resource_name().map_or(0, str::len))?
                .checked_add(filter.principal().map_or(0, str::len))?
                .checked_add(filter.host().map_or(0, str::len))
        },
    )
}

fn vector_bytes<T>(capacity: usize) -> Option<usize> {
    capacity.checked_mul(size_of::<T>())
}
