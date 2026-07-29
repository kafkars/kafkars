//! Atomic completion and retained-byte reservation before group-offset machine creation.

use core::mem::size_of;

use kafka_client_core::{
    ListConsumerGroupOffsetTarget, ListConsumerGroupOffsetsEffect, ListConsumerGroupOffsetsInput,
    ListConsumerGroupOffsetsMachine, ListConsumerGroupOffsetsPlan,
    ListConsumerGroupOffsetsSelection, Moment, OperationId,
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
        let owner_charge = request_owner_charge(&plan)
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
        let mut operation = ListConsumerGroupOffsetsOperation {
            operation_id,
            machine: ListConsumerGroupOffsetsMachine::new(operation_id, deadline.core(), plan),
            active_plan: None,
            completion_id,
            deadline,
            retained_bytes: LIST_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES,
            remaining_result_bytes: result_limit,
            submission: None,
            handoff: ListConsumerGroupOffsetsHandoff::Untouched,
            call: None,
            raw_terminal: None,
            terminal: None,
        };
        let start_result = start(&mut operation, now);
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
            operation.install_submission(operation_id, core_deadline, plan)?;
            Ok(false)
        }
        Some(ListConsumerGroupOffsetsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            operation.install_terminal(operation_id, terminal)?;
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

fn request_owner_charge(plan: &ListConsumerGroupOffsetsPlan) -> Option<usize> {
    let group_text_bytes = plan
        .group_ids()
        .iter()
        .try_fold(0usize, |total, group_id| total.checked_add(group_id.len()))?;
    let group_entry_bytes = plan
        .group_ids()
        .len()
        .checked_add(2)?
        .checked_mul(size_of::<String>())?;
    let duplicated_plan_bytes = group_text_bytes
        .checked_mul(3)?
        .checked_add(group_entry_bytes)?;
    let (selected_count, selected_topic_bytes) =
        plan.selections()
            .iter()
            .try_fold((0usize, 0usize), |(count, bytes), selection| {
                let ListConsumerGroupOffsetsSelection::Selected(targets) = selection else {
                    return Some((count, bytes));
                };
                Some((
                    count.checked_add(targets.len())?,
                    targets.iter().try_fold(bytes, |total, target| {
                        total.checked_add(target.topic().len())
                    })?,
                ))
            })?;
    let selected_plan_bytes = selected_topic_bytes.checked_mul(3)?.checked_add(
        selected_count.checked_mul(
            size_of::<ListConsumerGroupOffsetTarget>()
                .checked_mul(3)?
                // Conservatively covers the transient bounded topic-index tree,
                // exact response correlation position, and allocator overhead.
                .checked_add(size_of::<usize>().checked_mul(8)?)?,
        )?,
    )?;
    let selection_vector_bytes = plan
        .selections()
        .len()
        .checked_mul(size_of::<ListConsumerGroupOffsetsSelection>())?
        .checked_mul(3)?;
    let batch_outcome_slots = plan
        .group_ids()
        .len()
        .checked_mul(8usize.checked_mul(size_of::<usize>())?)?;
    size_of::<ListConsumerGroupOffsetsOperation>()
        .checked_add(size_of::<ListConsumerGroupOffsetsSubmission>())?
        .checked_add(duplicated_plan_bytes)
        .and_then(|charge| charge.checked_add(selected_plan_bytes))
        .and_then(|charge| charge.checked_add(selection_vector_bytes))
        .and_then(|charge| charge.checked_add(batch_outcome_slots))
}
