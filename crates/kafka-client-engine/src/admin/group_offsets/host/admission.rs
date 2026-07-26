//! Atomic completion and four-MiB envelope reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    ListConsumerGroupOffsetsEffect, ListConsumerGroupOffsetsInput, ListConsumerGroupOffsetsMachine,
    ListConsumerGroupOffsetsPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    LIST_CONSUMER_GROUP_OFFSETS_CAPACITY, LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES,
    ListConsumerGroupOffsetsAdmission, ListConsumerGroupOffsetsHandoff,
    ListConsumerGroupOffsetsHost, ListConsumerGroupOffsetsHostError,
    ListConsumerGroupOffsetsOperation, ListConsumerGroupOffsetsSubmission,
};
use crate::admin::group_offsets::{
    ListConsumerGroupOffsetsAdmissionErrorKind, ListConsumerGroupOffsetsObserver,
};

impl ListConsumerGroupOffsetsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ListConsumerGroupOffsetsPlan,
    ) -> Result<ListConsumerGroupOffsetsAdmission, ListConsumerGroupOffsetsAdmissionErrorKind> {
        if !self.accepting {
            return Err(ListConsumerGroupOffsetsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= LIST_CONSUMER_GROUP_OFFSETS_CAPACITY {
            return Err(ListConsumerGroupOffsetsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(ListConsumerGroupOffsetsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(plan.group_id().len())
            .ok_or(ListConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let result_limit = LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(ListConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES)
            .filter(|total| *total <= LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES)
            .ok_or(ListConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let group_id = plan.group_id().to_owned();
        let mut operation = ListConsumerGroupOffsetsOperation {
            operation_id,
            machine: ListConsumerGroupOffsetsMachine::new(operation_id, deadline.core(), plan),
            group_id,
            completion_id,
            deadline,
            retained_bytes: LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES,
            result_limit,
            submission: None,
            handoff: ListConsumerGroupOffsetsHandoff::Untouched,
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
        Ok(ListConsumerGroupOffsetsAdmission {
            observer: ListConsumerGroupOffsetsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut ListConsumerGroupOffsetsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, ListConsumerGroupOffsetsHostError> {
    let transition = operation
        .machine
        .apply(ListConsumerGroupOffsetsInput::Start { now })?;
    match transition.into_effect() {
        Some(ListConsumerGroupOffsetsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch);
            }
            operation.submission = Some(ListConsumerGroupOffsetsSubmission {
                operation_id,
                deadline,
                plan,
                result_limit: operation.result_limit,
            });
            Ok(false)
        }
        Some(ListConsumerGroupOffsetsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(ListConsumerGroupOffsetsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> ListConsumerGroupOffsetsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => ListConsumerGroupOffsetsAdmissionErrorKind::Capacity,
        _ => ListConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(group_bytes: usize) -> Option<usize> {
    size_of::<ListConsumerGroupOffsetsOperation>()
        .checked_add(size_of::<ListConsumerGroupOffsetsSubmission>())?
        .checked_add(2usize.checked_mul(size_of::<ListConsumerGroupOffsetsPlan>())?)?
        .checked_add(size_of::<String>())?
        .checked_add(3usize.checked_mul(group_bytes)?)
}
