//! Atomic completion, combined-envelope reservation, and linear submission handoff.

use core::mem::size_of;

use kafka_client_core::{
    AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsEffect,
    AlterConsumerGroupOffsetsInput, AlterConsumerGroupOffsetsMachine,
    AlterConsumerGroupOffsetsPlan, Moment, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionRegistryError,
    protocol::admin::group_offset_alter::{OffsetCommitTargetRef, generated_request_peak_charge},
};

use super::{
    ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY, ALTER_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES,
    AlterConsumerGroupOffsetsAdmission, AlterConsumerGroupOffsetsHandoff,
    AlterConsumerGroupOffsetsHost, AlterConsumerGroupOffsetsHostError,
    AlterConsumerGroupOffsetsOperation, AlterConsumerGroupOffsetsSubmission,
    model::AlterConsumerGroupOffsetsBounds,
};
use crate::admin::group_offset_alter::{
    AlterConsumerGroupOffsetsAdmissionErrorKind, AlterConsumerGroupOffsetsObserver,
};

impl AlterConsumerGroupOffsetsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterConsumerGroupOffsetsPlan,
    ) -> Result<AlterConsumerGroupOffsetsAdmission, AlterConsumerGroupOffsetsAdmissionErrorKind>
    {
        if !self.accepting {
            return Err(AlterConsumerGroupOffsetsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY {
            return Err(AlterConsumerGroupOffsetsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AlterConsumerGroupOffsetsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(AlterConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let target_refs_charge = target_refs_charge(plan.targets().len())
            .ok_or(AlterConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let generated_request_charge = generated_request_peak_charge(
            plan.group_id(),
            plan.targets().iter().map(|target| {
                OffsetCommitTargetRef::new(
                    target.topic(),
                    target.partition(),
                    target.next_offset(),
                    target.leader_epoch(),
                    target.metadata(),
                )
            }),
        )
        .ok_or(AlterConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let result_limit = ALTER_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .and_then(|limit| limit.checked_sub(target_refs_charge))
            .and_then(|limit| limit.checked_sub(generated_request_charge))
            .filter(|limit| *limit > 0)
            .ok_or(AlterConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let bounds = AlterConsumerGroupOffsetsBounds {
            request_scratch_limit: generated_request_charge,
            result_limit,
        };
        let total_bytes = self
            .retained_bytes
            .checked_add(ALTER_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES)
            .filter(|total| *total <= ALTER_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES)
            .ok_or(AlterConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let host_plan = plan.clone();
        let mut operation = AlterConsumerGroupOffsetsOperation {
            operation_id,
            machine: AlterConsumerGroupOffsetsMachine::new(operation_id, deadline.core(), plan),
            plan: host_plan,
            completion_id,
            deadline,
            retained_bytes: ALTER_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES,
            bounds,
            submission: None,
            rejected_submission: None,
            handoff: AlterConsumerGroupOffsetsHandoff::Untouched,
            call: None,
            recovered_call: None,
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
        Ok(AlterConsumerGroupOffsetsAdmission {
            observer: AlterConsumerGroupOffsetsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut AlterConsumerGroupOffsetsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AlterConsumerGroupOffsetsHostError> {
    let transition = operation
        .machine
        .apply(AlterConsumerGroupOffsetsInput::Start { now })?;
    match transition.into_effect() {
        Some(AlterConsumerGroupOffsetsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id
                || core_deadline != deadline.core()
                || operation.plan != plan
            {
                return Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch);
            }
            operation.submission = Some(AlterConsumerGroupOffsetsSubmission {
                operation_id,
                deadline,
                plan,
                bounds: operation.bounds,
            });
            Ok(false)
        }
        Some(AlterConsumerGroupOffsetsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(AlterConsumerGroupOffsetsHostError::MissingSubmission),
    }
}

fn reservation_error(
    error: CompletionRegistryError,
) -> AlterConsumerGroupOffsetsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AlterConsumerGroupOffsetsAdmissionErrorKind::Capacity,
        _ => AlterConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AlterConsumerGroupOffsetsPlan) -> Option<usize> {
    let text_bytes = plan
        .targets()
        .iter()
        .try_fold(plan.group_id().len(), |bytes, target| {
            bytes
                .checked_add(target.topic().len())
                .and_then(|bytes| bytes.checked_add(target.metadata().map_or(0, str::len)))
        })?;
    size_of::<AlterConsumerGroupOffsetsOperation>()
        .checked_add(size_of::<AlterConsumerGroupOffsetsSubmission>())?
        .checked_add(3usize.checked_mul(size_of::<AlterConsumerGroupOffsetsPlan>())?)?
        .checked_add(
            3usize.checked_mul(
                plan.targets()
                    .len()
                    .checked_mul(size_of::<AlterConsumerGroupOffsetTarget>())?,
            )?,
        )?
        .checked_add(3usize.checked_mul(text_bytes)?)
}

fn target_refs_charge(target_count: usize) -> Option<usize> {
    size_of::<Vec<OffsetCommitTargetRef<'static>>>()
        .checked_add(target_count.checked_mul(size_of::<OffsetCommitTargetRef<'static>>())?)
}
