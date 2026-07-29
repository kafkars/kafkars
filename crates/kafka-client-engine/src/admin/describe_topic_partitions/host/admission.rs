//! Atomic completion and four-MiB reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    DescribeTopicPartitionsEffect, DescribeTopicPartitionsInput, DescribeTopicPartitionsMachine,
    DescribeTopicPartitionsPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ADMIN_DESCRIBE_TOPIC_PARTITIONS_CAPACITY, ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES,
    AdminDescribeTopicPartitionsAdmission, AdminDescribeTopicPartitionsHandoff,
    AdminDescribeTopicPartitionsHost, AdminDescribeTopicPartitionsHostError,
    AdminDescribeTopicPartitionsOperation, AdminDescribeTopicPartitionsSubmission,
};
use crate::admin::describe_topic_partitions::{
    AdminDescribeTopicPartitionsAdmissionErrorKind, AdminDescribeTopicPartitionsObserver,
};

impl AdminDescribeTopicPartitionsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeTopicPartitionsPlan,
    ) -> Result<AdminDescribeTopicPartitionsAdmission, AdminDescribeTopicPartitionsAdmissionErrorKind>
    {
        if !self.accepting {
            return Err(AdminDescribeTopicPartitionsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ADMIN_DESCRIBE_TOPIC_PARTITIONS_CAPACITY {
            return Err(AdminDescribeTopicPartitionsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AdminDescribeTopicPartitionsAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(AdminDescribeTopicPartitionsAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(AdminDescribeTopicPartitionsAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES)
            .filter(|total| *total <= ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES)
            .ok_or(AdminDescribeTopicPartitionsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = AdminDescribeTopicPartitionsOperation {
            operation_id,
            machine: DescribeTopicPartitionsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES,
            remaining_result_bytes,
            submission: None,
            handoff: AdminDescribeTopicPartitionsHandoff::Untouched,
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
        Ok(AdminDescribeTopicPartitionsAdmission {
            observer: AdminDescribeTopicPartitionsObserver::from_completion(observer),
            fault,
        })
    }

    #[cfg(test)]
    pub(in crate::admin::describe_topic_partitions) const fn retained_bytes_for_test(
        &self,
    ) -> usize {
        self.retained_bytes
    }
}

fn start(
    operation: &mut AdminDescribeTopicPartitionsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AdminDescribeTopicPartitionsHostError> {
    let transition = operation
        .machine
        .apply(DescribeTopicPartitionsInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeTopicPartitionsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(AdminDescribeTopicPartitionsHostError::SubmissionMismatch);
            }
            operation.submission = Some(AdminDescribeTopicPartitionsSubmission {
                operation_id,
                deadline,
                plan,
                retained_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(DescribeTopicPartitionsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(AdminDescribeTopicPartitionsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(AdminDescribeTopicPartitionsHostError::MissingSubmission),
    }
}

fn reservation_error(
    error: CompletionRegistryError,
) -> AdminDescribeTopicPartitionsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AdminDescribeTopicPartitionsAdmissionErrorKind::Capacity,
        _ => AdminDescribeTopicPartitionsAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &DescribeTopicPartitionsPlan) -> Option<usize> {
    let topic_owners = plan.topics().len().checked_mul(size_of::<String>())?;
    let topic_bytes = plan
        .topics()
        .iter()
        .try_fold(0usize, |bytes, topic| bytes.checked_add(topic.len()))?;
    let cursor_bytes = plan.cursor().map_or(0, |cursor| cursor.topic_name().len());
    let dynamic = topic_owners
        .checked_add(topic_bytes)?
        .checked_add(cursor_bytes)?;
    size_of::<AdminDescribeTopicPartitionsOperation>()
        .checked_add(size_of::<AdminDescribeTopicPartitionsSubmission>())?
        .checked_add(2usize.checked_mul(size_of::<DescribeTopicPartitionsPlan>())?)?
        .checked_add(2usize.checked_mul(dynamic)?)
}
