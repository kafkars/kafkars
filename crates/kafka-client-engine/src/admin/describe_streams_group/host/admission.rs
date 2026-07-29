//! Atomic completion, request, and four-MiB result reservation.

use core::mem::size_of;

use kafka_client_core::{
    DescribeStreamsGroupInput, DescribeStreamsGroupMachine, DescribeStreamsGroupPlan, Moment,
    OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DESCRIBE_STREAMS_GROUP_CAPACITY, DESCRIBE_STREAMS_GROUP_RESULT_BYTES,
    DESCRIBE_STREAMS_GROUP_RETAINED_BYTES, DescribeStreamsGroupAdmission,
    DescribeStreamsGroupHandoff, DescribeStreamsGroupHost, DescribeStreamsGroupOperation,
    DescribeStreamsGroupSubmission,
};
use crate::admin::describe_streams_group::{
    DescribeStreamsGroupAdmissionErrorKind, DescribeStreamsGroupObserver,
};

impl DescribeStreamsGroupHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeStreamsGroupPlan,
    ) -> Result<DescribeStreamsGroupAdmission, DescribeStreamsGroupAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeStreamsGroupAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DESCRIBE_STREAMS_GROUP_CAPACITY {
            return Err(DescribeStreamsGroupAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeStreamsGroupAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(DescribeStreamsGroupAdmissionErrorKind::RetainedBytes)?;
        let operation_bytes = owner_charge
            .checked_add(DESCRIBE_STREAMS_GROUP_RESULT_BYTES)
            .ok_or(DescribeStreamsGroupAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(operation_bytes)
            .filter(|total| *total <= DESCRIBE_STREAMS_GROUP_RETAINED_BYTES)
            .ok_or(DescribeStreamsGroupAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let operation = DescribeStreamsGroupOperation {
            operation_id,
            machine: DescribeStreamsGroupMachine::new(operation_id, deadline.core(), plan),
            active_plan: None,
            completion_id,
            deadline,
            retained_bytes: operation_bytes,
            remaining_result_bytes: DESCRIBE_STREAMS_GROUP_RESULT_BYTES,
            submission: None,
            handoff: DescribeStreamsGroupHandoff::Untouched,
            call: None,
            raw_terminal: None,
            terminal: None,
        };
        self.operations.push(operation);
        let fault = self
            .apply(operation_id, DescribeStreamsGroupInput::Start { now })
            .err();
        if let Some(error) = fault {
            self.health = Some(error);
        }
        Ok(DescribeStreamsGroupAdmission {
            observer: DescribeStreamsGroupObserver::from_completion(observer),
            fault,
        })
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeStreamsGroupAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeStreamsGroupAdmissionErrorKind::Capacity,
        _ => DescribeStreamsGroupAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &DescribeStreamsGroupPlan) -> Option<usize> {
    let group_ids = plan.group_ids();
    let group_text_bytes = group_ids
        .iter()
        .try_fold(0usize, |total, group_id| total.checked_add(group_id.len()))?;
    let group_slots = group_ids
        .len()
        .checked_add(2)?
        .checked_mul(size_of::<String>())?;
    let active_group_bytes = group_ids
        .iter()
        .map(String::len)
        .max()
        .unwrap_or(0)
        .checked_mul(2)?;
    size_of::<DescribeStreamsGroupOperation>()
        .checked_add(size_of::<DescribeStreamsGroupSubmission>())?
        .checked_add(group_text_bytes)?
        .checked_add(group_slots)?
        .checked_add(active_group_bytes)
}
