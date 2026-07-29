//! Atomic completion, request, and four-MiB result reservation.

use core::mem::size_of;

use kafka_client_core::{
    DescribeShareGroupEffect, DescribeShareGroupInput, DescribeShareGroupMachine,
    DescribeShareGroupPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DESCRIBE_SHARE_GROUP_CAPACITY, DESCRIBE_SHARE_GROUP_RESULT_BYTES,
    DESCRIBE_SHARE_GROUP_RETAINED_BYTES, DescribeShareGroupAdmission, DescribeShareGroupHandoff,
    DescribeShareGroupHost, DescribeShareGroupHostError, DescribeShareGroupOperation,
    DescribeShareGroupSubmission,
};
use crate::admin::describe_share_group::{
    DescribeShareGroupAdmissionErrorKind, DescribeShareGroupObserver,
};

impl DescribeShareGroupHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeShareGroupPlan,
    ) -> Result<DescribeShareGroupAdmission, DescribeShareGroupAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeShareGroupAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DESCRIBE_SHARE_GROUP_CAPACITY {
            return Err(DescribeShareGroupAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeShareGroupAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(DescribeShareGroupAdmissionErrorKind::RetainedBytes)?;
        let operation_bytes = owner_charge
            .checked_add(DESCRIBE_SHARE_GROUP_RESULT_BYTES)
            .ok_or(DescribeShareGroupAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(operation_bytes)
            .filter(|total| *total <= DESCRIBE_SHARE_GROUP_RETAINED_BYTES)
            .ok_or(DescribeShareGroupAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = DescribeShareGroupOperation {
            operation_id,
            machine: DescribeShareGroupMachine::new(operation_id, deadline.core(), plan),
            active_plan: None,
            completion_id,
            deadline,
            retained_bytes: operation_bytes,
            remaining_result_bytes: DESCRIBE_SHARE_GROUP_RESULT_BYTES,
            submission: None,
            handoff: DescribeShareGroupHandoff::Untouched,
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
        Ok(DescribeShareGroupAdmission {
            observer: DescribeShareGroupObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeShareGroupOperation,
    now: Moment,
) -> Result<bool, DescribeShareGroupHostError> {
    let transition = operation
        .machine
        .apply(DescribeShareGroupInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeShareGroupEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            operation.install_submission(operation_id, core_deadline, plan)?;
            Ok(false)
        }
        Some(DescribeShareGroupEffect::Complete {
            operation_id,
            terminal,
        }) => {
            operation.install_terminal(operation_id, terminal)?;
            Ok(true)
        }
        None => Err(DescribeShareGroupHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeShareGroupAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeShareGroupAdmissionErrorKind::Capacity,
        _ => DescribeShareGroupAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &DescribeShareGroupPlan) -> Option<usize> {
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
    size_of::<DescribeShareGroupOperation>()
        .checked_add(size_of::<DescribeShareGroupSubmission>())?
        .checked_add(duplicated_plan_bytes)
}
