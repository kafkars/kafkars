//! Atomic completion, request, and four-MiB result reservation.

use core::mem::size_of;

use kafka_client_core::{
    ListShareGroupOffsetTarget, ListShareGroupOffsetsEffect, ListShareGroupOffsetsInput,
    ListShareGroupOffsetsMachine, ListShareGroupOffsetsPlan, ListShareGroupOffsetsSelection,
    Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    LIST_SHARE_GROUP_OFFSETS_CAPACITY, LIST_SHARE_GROUP_OFFSETS_RESULT_BYTES,
    LIST_SHARE_GROUP_OFFSETS_RETAINED_BYTES, ListShareGroupOffsetsAdmission,
    ListShareGroupOffsetsHandoff, ListShareGroupOffsetsHost, ListShareGroupOffsetsHostError,
    ListShareGroupOffsetsOperation, ListShareGroupOffsetsSubmission,
};
use crate::admin::list_share_group_offsets::{
    ListShareGroupOffsetsAdmissionErrorKind, ListShareGroupOffsetsObserver,
};

impl ListShareGroupOffsetsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ListShareGroupOffsetsPlan,
    ) -> Result<ListShareGroupOffsetsAdmission, ListShareGroupOffsetsAdmissionErrorKind> {
        if !self.accepting {
            return Err(ListShareGroupOffsetsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= LIST_SHARE_GROUP_OFFSETS_CAPACITY {
            return Err(ListShareGroupOffsetsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(ListShareGroupOffsetsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(ListShareGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let operation_bytes = owner_charge
            .checked_add(LIST_SHARE_GROUP_OFFSETS_RESULT_BYTES)
            .ok_or(ListShareGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(operation_bytes)
            .filter(|total| *total <= LIST_SHARE_GROUP_OFFSETS_RETAINED_BYTES)
            .ok_or(ListShareGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = ListShareGroupOffsetsOperation {
            operation_id,
            machine: ListShareGroupOffsetsMachine::new(operation_id, deadline.core(), plan),
            active_plan: None,
            completion_id,
            deadline,
            retained_bytes: operation_bytes,
            remaining_result_bytes: LIST_SHARE_GROUP_OFFSETS_RESULT_BYTES,
            submission: None,
            handoff: ListShareGroupOffsetsHandoff::Untouched,
            call: None,
            recovered_call: None,
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
        if terminal_ready && let Err(error) = self.publish_terminal(self.operations.len() - 1) {
            self.health = Some(error);
            fault = Some(error);
        }
        Ok(ListShareGroupOffsetsAdmission {
            observer: ListShareGroupOffsetsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut ListShareGroupOffsetsOperation,
    now: Moment,
) -> Result<bool, ListShareGroupOffsetsHostError> {
    let transition = operation
        .machine
        .apply(ListShareGroupOffsetsInput::Start { now })?;
    match transition.into_effect() {
        Some(ListShareGroupOffsetsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            operation.install_submission(operation_id, core_deadline, plan)?;
            Ok(false)
        }
        Some(ListShareGroupOffsetsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            operation.install_terminal(operation_id, terminal)?;
            Ok(true)
        }
        None => Err(ListShareGroupOffsetsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> ListShareGroupOffsetsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => ListShareGroupOffsetsAdmissionErrorKind::Capacity,
        _ => ListShareGroupOffsetsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &ListShareGroupOffsetsPlan) -> Option<usize> {
    let mut text_bytes = 0usize;
    let mut target_entries = 0usize;
    for query in plan.queries() {
        text_bytes = text_bytes.checked_add(query.group_id().len())?;
        if let ListShareGroupOffsetsSelection::Selected(targets) = query.selection() {
            target_entries = target_entries.checked_add(targets.len())?;
            text_bytes = targets.iter().try_fold(text_bytes, |total, target| {
                total.checked_add(target.topic().len())
            })?;
        }
    }
    let group_entries = plan
        .queries()
        .len()
        .checked_add(2)?
        .checked_mul(size_of::<kafka_client_core::ListShareGroupOffsetsQuery>())?;
    let target_bytes = target_entries.checked_mul(size_of::<ListShareGroupOffsetTarget>())?;
    let duplicated_plan_bytes = text_bytes
        .checked_add(target_bytes)?
        .checked_mul(3)?
        .checked_add(group_entries)?;
    size_of::<ListShareGroupOffsetsOperation>()
        .checked_add(size_of::<ListShareGroupOffsetsSubmission>())?
        .checked_add(duplicated_plan_bytes)
}
