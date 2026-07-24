//! Atomic reservation and deterministic start of one `DescribeCluster` call.

use kafka_client_core::{
    DescribeClusterEffect, DescribeClusterInput, DescribeClusterMachine, Moment,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DESCRIBE_CLUSTER_OPERATION_BYTES, DESCRIBE_CLUSTER_RETAINED_BYTES, DescribeClusterAdmission,
    DescribeClusterHost, DescribeClusterHostError, DescribeClusterOperation,
    DescribeClusterSubmission,
};
use crate::admin::{DescribeClusterAdmissionErrorKind, DescribeClusterObserver};

impl DescribeClusterHost {
    #[cfg(test)]
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<DescribeClusterAdmission, DescribeClusterAdmissionErrorKind> {
        self.try_admit_with_options(now, deadline, false, false)
    }

    pub(crate) fn try_admit_with_options(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        include_fenced_brokers: bool,
        include_authorized_operations: bool,
    ) -> Result<DescribeClusterAdmission, DescribeClusterAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeClusterAdmissionErrorKind::Closed);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeClusterAdmissionErrorKind::IdentityExhausted)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(DESCRIBE_CLUSTER_OPERATION_BYTES)
            .ok_or(DescribeClusterAdmissionErrorKind::RetainedBytes)?;
        if total_bytes > DESCRIBE_CLUSTER_RETAINED_BYTES {
            return Err(DescribeClusterAdmissionErrorKind::RetainedBytes);
        }
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;
        self.next_operation_id = operation_id
            .get()
            .checked_add(1)
            .map(kafka_client_core::OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = DescribeClusterOperation {
            operation_id,
            machine: DescribeClusterMachine::new_with_options(
                operation_id,
                deadline.core(),
                include_fenced_brokers,
                include_authorized_operations,
            ),
            completion_id,
            deadline,
            retained_bytes: DESCRIBE_CLUSTER_OPERATION_BYTES,
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
        Ok(DescribeClusterAdmission {
            observer: DescribeClusterObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeClusterOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DescribeClusterHostError> {
    let transition = operation
        .machine
        .apply(DescribeClusterInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeClusterEffect::Submit {
            operation_id,
            include_fenced_brokers,
            include_authorized_operations,
            ..
        }) => {
            operation.submission = Some(DescribeClusterSubmission {
                operation_id,
                deadline,
                retained_bytes: operation.retained_bytes,
                include_fenced_brokers,
                include_authorized_operations,
            });
            Ok(false)
        }
        Some(DescribeClusterEffect::Complete { terminal, .. }) => {
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeClusterHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeClusterAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeClusterAdmissionErrorKind::Capacity,
        _ => DescribeClusterAdmissionErrorKind::HostUnavailable,
    }
}
