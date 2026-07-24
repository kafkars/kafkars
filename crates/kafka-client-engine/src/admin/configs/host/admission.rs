//! Atomic reservation and deterministic startup for one `DescribeConfigs` operation.

use kafka_client_core::{
    DescribeConfigsEffect, DescribeConfigsInput, DescribeConfigsMachine, DescribeConfigsPlan,
    Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DESCRIBE_CONFIGS_RETAINED_BYTES, DescribeConfigsAdmission, DescribeConfigsHost,
    DescribeConfigsHostError, DescribeConfigsOperation, DescribeConfigsSubmission,
};
use crate::admin::{
    DescribeConfigsAdmissionErrorKind, DescribeConfigsObserver, DescribeConfigsRetention,
};

impl DescribeConfigsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeConfigsPlan,
        retention: DescribeConfigsRetention,
    ) -> Result<DescribeConfigsAdmission, DescribeConfigsAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeConfigsAdmissionErrorKind::Closed);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeConfigsAdmissionErrorKind::IdentityExhausted)?;
        let Some(total_bytes) = self.retained_bytes.checked_add(retention.total()) else {
            return Err(DescribeConfigsAdmissionErrorKind::RetainedBytes);
        };
        if total_bytes > DESCRIBE_CONFIGS_RETAINED_BYTES {
            return Err(DescribeConfigsAdmissionErrorKind::RetainedBytes);
        }
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;
        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = DescribeConfigsOperation {
            operation_id,
            machine: DescribeConfigsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: retention.total(),
            result_limit: retention.result_limit(),
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
        Ok(DescribeConfigsAdmission {
            observer: DescribeConfigsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeConfigsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DescribeConfigsHostError> {
    let transition = operation
        .machine
        .apply(DescribeConfigsInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeConfigsEffect::Submit {
            operation_id, plan, ..
        }) => {
            operation.submission = Some(DescribeConfigsSubmission {
                operation_id,
                deadline,
                plan,
                result_limit: operation.result_limit,
            });
            Ok(false)
        }
        Some(DescribeConfigsEffect::Complete { terminal, .. }) => {
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeConfigsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeConfigsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeConfigsAdmissionErrorKind::Capacity,
        _ => DescribeConfigsAdmissionErrorKind::HostUnavailable,
    }
}
