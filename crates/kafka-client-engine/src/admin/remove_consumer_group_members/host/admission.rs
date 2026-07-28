//! Atomic completion and retained-envelope reservation for member removal.

use core::mem::size_of;

use kafka_client_core::{
    ConsumerGroupMemberRemoval, Moment, OperationId, RemoveConsumerGroupMembersEffect,
    RemoveConsumerGroupMembersInput, RemoveConsumerGroupMembersMachine,
    RemoveConsumerGroupMembersPlan,
};

use crate::{
    admin::remove_consumer_group_members::{
        RemoveConsumerGroupMembersAdmissionErrorKind, RemoveConsumerGroupMembersObserver,
    },
    clock::OperationDeadline,
    completion::CompletionRegistryError,
    protocol::admin::remove_consumer_group_members::remove_consumer_group_members_request_charge,
};

use super::{
    REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY, REMOVE_CONSUMER_GROUP_MEMBERS_RETAINED_BYTES,
    RemoveConsumerGroupMembersAdmission, RemoveConsumerGroupMembersHandoff,
    RemoveConsumerGroupMembersHost, RemoveConsumerGroupMembersOperation,
    RemoveConsumerGroupMembersSubmission,
};

impl RemoveConsumerGroupMembersHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: RemoveConsumerGroupMembersPlan,
    ) -> Result<RemoveConsumerGroupMembersAdmission, RemoveConsumerGroupMembersAdmissionErrorKind>
    {
        if !self.accepting {
            return Err(RemoveConsumerGroupMembersAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY {
            return Err(RemoveConsumerGroupMembersAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(RemoveConsumerGroupMembersAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(RemoveConsumerGroupMembersAdmissionErrorKind::RetainedBytes)?;
        let request_charge = remove_consumer_group_members_request_charge(&plan)
            .ok_or(RemoveConsumerGroupMembersAdmissionErrorKind::RetainedBytes)?;
        let result_limit = REMOVE_CONSUMER_GROUP_MEMBERS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .and_then(|limit| limit.checked_sub(request_charge))
            .filter(|limit| *limit > 0)
            .ok_or(RemoveConsumerGroupMembersAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(REMOVE_CONSUMER_GROUP_MEMBERS_RETAINED_BYTES)
            .filter(|total| *total <= REMOVE_CONSUMER_GROUP_MEMBERS_RETAINED_BYTES)
            .ok_or(RemoveConsumerGroupMembersAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let response_plan = plan.clone();
        let mut operation = RemoveConsumerGroupMembersOperation {
            operation_id,
            machine: RemoveConsumerGroupMembersMachine::new(operation_id, deadline.core(), plan),
            response_plan,
            completion_id,
            deadline,
            retained_bytes: REMOVE_CONSUMER_GROUP_MEMBERS_RETAINED_BYTES,
            request_scratch_limit: request_charge,
            result_limit,
            submission: None,
            rejected_submission: None,
            handoff: RemoveConsumerGroupMembersHandoff::Untouched,
            call: None,
            recovered_call: None,
            raw_terminal: None,
            terminal: None,
        };
        let transition = operation
            .machine
            .apply(RemoveConsumerGroupMembersInput::Start { now })
            .map_err(|_error| RemoveConsumerGroupMembersAdmissionErrorKind::HostUnavailable)?;
        match transition.into_effect() {
            Some(RemoveConsumerGroupMembersEffect::Submit {
                operation_id: submitted_id,
                deadline: core_deadline,
                plan,
            }) if submitted_id == operation_id && core_deadline == deadline.core() => {
                operation.submission = Some(RemoveConsumerGroupMembersSubmission {
                    operation_id,
                    deadline,
                    plan,
                    request_scratch_limit: request_charge,
                    result_limit,
                });
            }
            Some(RemoveConsumerGroupMembersEffect::Complete {
                operation_id: completed_id,
                terminal,
            }) if completed_id == operation_id => {
                operation.terminal = Some(terminal);
            }
            _ => return Err(RemoveConsumerGroupMembersAdmissionErrorKind::HostUnavailable),
        }
        let terminal_ready = operation.terminal.is_some();
        self.operations.push(operation);
        let mut fault = None;
        if terminal_ready {
            if let Err(error) = self.publish_terminal(self.operations.len() - 1) {
                self.health = Some(error);
                fault = Some(error);
            }
        }
        Ok(RemoveConsumerGroupMembersAdmission {
            observer: RemoveConsumerGroupMembersObserver::from_completion(observer),
            fault,
        })
    }
}

fn reservation_error(
    error: CompletionRegistryError,
) -> RemoveConsumerGroupMembersAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => RemoveConsumerGroupMembersAdmissionErrorKind::Capacity,
        _ => RemoveConsumerGroupMembersAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &RemoveConsumerGroupMembersPlan) -> Option<usize> {
    let payload = plan
        .members()
        .iter()
        .try_fold(plan.group_id().len(), |bytes, member| {
            bytes.checked_add(member.group_instance_id().len())
        })?
        .checked_add(plan.reason().map_or(0, str::len))?;
    size_of::<RemoveConsumerGroupMembersOperation>()
        .checked_add(size_of::<RemoveConsumerGroupMembersSubmission>())?
        .checked_add(3usize.checked_mul(size_of::<RemoveConsumerGroupMembersPlan>())?)?
        .checked_add(
            3usize.checked_mul(
                plan.members()
                    .len()
                    .checked_mul(size_of::<ConsumerGroupMemberRemoval>())?,
            )?,
        )?
        .checked_add(3usize.checked_mul(payload)?)
}
