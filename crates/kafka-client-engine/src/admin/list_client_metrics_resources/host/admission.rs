//! Atomic completion and four-MiB envelope reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    ListClientMetricsResourcesEffect, ListClientMetricsResourcesInput,
    ListClientMetricsResourcesMachine, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    LIST_CLIENT_METRICS_RESOURCES_CAPACITY, LIST_CLIENT_METRICS_RESOURCES_RETAINED_BYTES,
    ListClientMetricsResourcesAdmission, ListClientMetricsResourcesHandoff,
    ListClientMetricsResourcesHost, ListClientMetricsResourcesHostError,
    ListClientMetricsResourcesOperation, ListClientMetricsResourcesSubmission,
};
use crate::admin::list_client_metrics_resources::{
    ListClientMetricsResourcesAdmissionErrorKind, ListClientMetricsResourcesObserver,
};

impl ListClientMetricsResourcesHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<ListClientMetricsResourcesAdmission, ListClientMetricsResourcesAdmissionErrorKind>
    {
        if !self.accepting {
            return Err(ListClientMetricsResourcesAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= LIST_CLIENT_METRICS_RESOURCES_CAPACITY {
            return Err(ListClientMetricsResourcesAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(ListClientMetricsResourcesAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge()
            .ok_or(ListClientMetricsResourcesAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = LIST_CLIENT_METRICS_RESOURCES_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(ListClientMetricsResourcesAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(LIST_CLIENT_METRICS_RESOURCES_RETAINED_BYTES)
            .filter(|total| *total <= LIST_CLIENT_METRICS_RESOURCES_RETAINED_BYTES)
            .ok_or(ListClientMetricsResourcesAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = ListClientMetricsResourcesOperation {
            operation_id,
            machine: ListClientMetricsResourcesMachine::new(operation_id, deadline.core()),
            completion_id,
            deadline,
            retained_bytes: LIST_CLIENT_METRICS_RESOURCES_RETAINED_BYTES,
            remaining_result_bytes,
            submission: None,
            handoff: ListClientMetricsResourcesHandoff::Untouched,
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
        if terminal_ready {
            if let Err(error) = self.publish_terminal(self.operations.len() - 1) {
                self.health = Some(error);
                fault = Some(error);
            }
        }
        Ok(ListClientMetricsResourcesAdmission {
            observer: ListClientMetricsResourcesObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut ListClientMetricsResourcesOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, ListClientMetricsResourcesHostError> {
    let transition = operation
        .machine
        .apply(ListClientMetricsResourcesInput::Start { now })?;
    match transition.into_effect() {
        Some(ListClientMetricsResourcesEffect::Submit {
            operation_id,
            deadline: core_deadline,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(ListClientMetricsResourcesHostError::SubmissionMismatch);
            }
            operation.submission = Some(ListClientMetricsResourcesSubmission {
                operation_id,
                deadline,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(ListClientMetricsResourcesEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(ListClientMetricsResourcesHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(ListClientMetricsResourcesHostError::MissingSubmission),
    }
}

fn reservation_error(
    error: CompletionRegistryError,
) -> ListClientMetricsResourcesAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => ListClientMetricsResourcesAdmissionErrorKind::Capacity,
        _ => ListClientMetricsResourcesAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge() -> Option<usize> {
    size_of::<ListClientMetricsResourcesOperation>()
        .checked_add(size_of::<ListClientMetricsResourcesSubmission>())
}
