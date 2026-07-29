//! Atomic terminal and retained-byte reservation before machine construction.

use core::mem::size_of;

use kafka_client_core::{
    AdminGroupListingFilters, AdminGroupListingScope, AdminListConsumerGroupsEffect,
    AdminListConsumerGroupsInput, AdminListConsumerGroupsMachine, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    LIST_CONSUMER_GROUPS_CAPACITY, LIST_CONSUMER_GROUPS_RETAINED_BYTES,
    ListConsumerGroupsAdmission, ListConsumerGroupsHandoff, ListConsumerGroupsHost,
    ListConsumerGroupsHostError, ListConsumerGroupsOperation, ListConsumerGroupsSubmission,
    ListConsumerGroupsSubmissionKind,
};
use crate::admin::list_consumer_groups::{
    ListConsumerGroupsAdmissionErrorKind, ListConsumerGroupsObserver,
};

impl ListConsumerGroupsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        scope: AdminGroupListingScope,
        filters: AdminGroupListingFilters,
    ) -> Result<ListConsumerGroupsAdmission, ListConsumerGroupsAdmissionErrorKind> {
        if !self.accepting {
            return Err(ListConsumerGroupsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= LIST_CONSUMER_GROUPS_CAPACITY {
            return Err(ListConsumerGroupsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(ListConsumerGroupsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&filters)
            .ok_or(ListConsumerGroupsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = LIST_CONSUMER_GROUPS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .ok_or(ListConsumerGroupsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(LIST_CONSUMER_GROUPS_RETAINED_BYTES)
            .filter(|total| *total <= LIST_CONSUMER_GROUPS_RETAINED_BYTES)
            .ok_or(ListConsumerGroupsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = ListConsumerGroupsOperation {
            operation_id,
            machine: AdminListConsumerGroupsMachine::new(
                operation_id,
                deadline.core(),
                scope,
                filters,
            ),
            completion_id,
            deadline,
            retained_bytes: LIST_CONSUMER_GROUPS_RETAINED_BYTES,
            remaining_result_bytes,
            submission: None,
            rejected_submission: None,
            handoff: ListConsumerGroupsHandoff::Untouched,
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
        Ok(ListConsumerGroupsAdmission {
            observer: ListConsumerGroupsObserver::from_completion(observer),
            fault,
        })
    }
}

fn request_owner_charge(filters: &AdminGroupListingFilters) -> Option<usize> {
    let filter_owners = filters
        .state_filters()
        .len()
        .checked_add(filters.group_type_filters().len())?
        .checked_add(filters.protocol_type_filters().len())?
        .checked_mul(size_of::<String>())?;
    let filter_text = filters
        .state_filters()
        .iter()
        .chain(filters.group_type_filters())
        .chain(filters.protocol_type_filters())
        .try_fold(0usize, |bytes, filter| bytes.checked_add(filter.len()))?;
    let filter_bytes = filter_owners.checked_add(filter_text)?;
    size_of::<ListConsumerGroupsOperation>()
        .checked_add(size_of::<ListConsumerGroupsSubmission>())?
        .checked_add(2usize.checked_mul(size_of::<AdminGroupListingFilters>())?)?
        .checked_add(3usize.checked_mul(filter_bytes)?)
}

fn start(
    operation: &mut ListConsumerGroupsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, ListConsumerGroupsHostError> {
    let transition = operation
        .machine
        .apply(AdminListConsumerGroupsInput::Start { now })?;
    match transition.into_effect() {
        Some(AdminListConsumerGroupsEffect::SubmitDiscovery {
            operation_id,
            deadline: core_deadline,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(ListConsumerGroupsHostError::SubmissionMismatch);
            }
            operation.submission = Some(ListConsumerGroupsSubmission {
                operation_id,
                deadline,
                kind: ListConsumerGroupsSubmissionKind::Discovery,
            });
            Ok(false)
        }
        Some(AdminListConsumerGroupsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(ListConsumerGroupsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        _ => Err(ListConsumerGroupsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> ListConsumerGroupsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => ListConsumerGroupsAdmissionErrorKind::Capacity,
        _ => ListConsumerGroupsAdmissionErrorKind::HostUnavailable,
    }
}
