//! Atomic completion, request, and two-MiB result reservation.

use core::mem::size_of;

use kafka_client_core::{
    ConfigResourceType, ListConfigResourcesEffect, ListConfigResourcesInput,
    ListConfigResourcesMachine, ListConfigResourcesPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    LIST_CONFIG_RESOURCES_CAPACITY, LIST_CONFIG_RESOURCES_RESULT_BYTES,
    LIST_CONFIG_RESOURCES_RETAINED_BYTES, ListConfigResourcesAdmission, ListConfigResourcesHandoff,
    ListConfigResourcesHost, ListConfigResourcesHostError, ListConfigResourcesOperation,
    ListConfigResourcesSubmission,
};
use crate::admin::list_config_resources::{
    ListConfigResourcesAdmissionErrorKind, ListConfigResourcesObserver,
};

impl ListConfigResourcesHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ListConfigResourcesPlan,
    ) -> Result<ListConfigResourcesAdmission, ListConfigResourcesAdmissionErrorKind> {
        if !self.accepting {
            return Err(ListConfigResourcesAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= LIST_CONFIG_RESOURCES_CAPACITY {
            return Err(ListConfigResourcesAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(ListConfigResourcesAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(ListConfigResourcesAdmissionErrorKind::RetainedBytes)?;
        let operation_bytes = owner_charge
            .checked_add(LIST_CONFIG_RESOURCES_RESULT_BYTES)
            .ok_or(ListConfigResourcesAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(operation_bytes)
            .filter(|total| *total <= LIST_CONFIG_RESOURCES_RETAINED_BYTES)
            .ok_or(ListConfigResourcesAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = ListConfigResourcesOperation {
            operation_id,
            machine: ListConfigResourcesMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: operation_bytes,
            remaining_result_bytes: LIST_CONFIG_RESOURCES_RESULT_BYTES,
            submission: None,
            handoff: ListConfigResourcesHandoff::Untouched,
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
        Ok(ListConfigResourcesAdmission {
            observer: ListConfigResourcesObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut ListConfigResourcesOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, ListConfigResourcesHostError> {
    let transition = operation
        .machine
        .apply(ListConfigResourcesInput::Start { now })?;
    match transition.into_effect() {
        Some(ListConfigResourcesEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(ListConfigResourcesHostError::SubmissionMismatch);
            }
            operation.submission = Some(ListConfigResourcesSubmission {
                operation_id,
                deadline,
                plan,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(ListConfigResourcesEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(ListConfigResourcesHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(ListConfigResourcesHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> ListConfigResourcesAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => ListConfigResourcesAdmissionErrorKind::Capacity,
        _ => ListConfigResourcesAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &ListConfigResourcesPlan) -> Option<usize> {
    let plan_storage = plan
        .resource_types()
        .len()
        .checked_mul(size_of::<ConfigResourceType>())?
        .checked_mul(2)?;
    size_of::<ListConfigResourcesOperation>()
        .checked_add(size_of::<ListConfigResourcesSubmission>())?
        .checked_add(plan_storage)
}
