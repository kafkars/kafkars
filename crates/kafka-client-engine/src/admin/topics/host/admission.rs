//! Atomic reservation and deterministic start of one `DescribeTopics` operation.

use kafka_client_core::{
    DescribeTopicsEffect, DescribeTopicsInput, DescribeTopicsMachine, DescribeTopicsPlan, Moment,
    OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DESCRIBE_TOPICS_RETAINED_BYTES, DescribeTopicsAdmission, DescribeTopicsHost,
    DescribeTopicsHostError, DescribeTopicsOperation, DescribeTopicsSubmission,
};
use crate::admin::{DescribeTopicsAdmissionErrorKind, DescribeTopicsObserver};

impl DescribeTopicsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeTopicsPlan,
        retained_bytes: usize,
    ) -> Result<DescribeTopicsAdmission, DescribeTopicsAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeTopicsAdmissionErrorKind::Closed);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeTopicsAdmissionErrorKind::IdentityExhausted)?;
        let Some(total_bytes) = self.retained_bytes.checked_add(retained_bytes) else {
            return Err(DescribeTopicsAdmissionErrorKind::RetainedBytes);
        };
        if total_bytes > DESCRIBE_TOPICS_RETAINED_BYTES {
            return Err(DescribeTopicsAdmissionErrorKind::RetainedBytes);
        }
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;
        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = DescribeTopicsOperation {
            operation_id,
            machine: DescribeTopicsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes,
            submission: None,
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
        Ok(DescribeTopicsAdmission {
            observer: DescribeTopicsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeTopicsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DescribeTopicsHostError> {
    let transition = operation
        .machine
        .apply(DescribeTopicsInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeTopicsEffect::Submit {
            operation_id, plan, ..
        }) => {
            operation.submission = Some(DescribeTopicsSubmission {
                operation_id,
                deadline,
                plan,
                retained_bytes: operation.retained_bytes,
            });
            Ok(false)
        }
        Some(DescribeTopicsEffect::Complete { terminal, .. }) => {
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeTopicsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeTopicsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeTopicsAdmissionErrorKind::Capacity,
        _ => DescribeTopicsAdmissionErrorKind::HostUnavailable,
    }
}
