//! Atomic completion and four-MiB envelope reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    DescribeMetadataQuorumEffect, DescribeMetadataQuorumInput, DescribeMetadataQuorumMachine,
    Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DESCRIBE_METADATA_QUORUM_CAPACITY, DESCRIBE_METADATA_QUORUM_RETAINED_BYTES,
    DescribeMetadataQuorumAdmission, DescribeMetadataQuorumHandoff, DescribeMetadataQuorumHost,
    DescribeMetadataQuorumHostError, DescribeMetadataQuorumOperation,
    DescribeMetadataQuorumSubmission,
};
use crate::admin::describe_metadata_quorum::{
    DescribeMetadataQuorumAdmissionErrorKind, DescribeMetadataQuorumObserver,
};

impl DescribeMetadataQuorumHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<DescribeMetadataQuorumAdmission, DescribeMetadataQuorumAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeMetadataQuorumAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DESCRIBE_METADATA_QUORUM_CAPACITY {
            return Err(DescribeMetadataQuorumAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeMetadataQuorumAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge()
            .ok_or(DescribeMetadataQuorumAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = DESCRIBE_METADATA_QUORUM_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(DescribeMetadataQuorumAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(DESCRIBE_METADATA_QUORUM_RETAINED_BYTES)
            .filter(|total| *total <= DESCRIBE_METADATA_QUORUM_RETAINED_BYTES)
            .ok_or(DescribeMetadataQuorumAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = DescribeMetadataQuorumOperation {
            operation_id,
            machine: DescribeMetadataQuorumMachine::new(operation_id, deadline.core()),
            completion_id,
            deadline,
            retained_bytes: DESCRIBE_METADATA_QUORUM_RETAINED_BYTES,
            remaining_result_bytes,
            submission: None,
            handoff: DescribeMetadataQuorumHandoff::Untouched,
            call: None,
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
        Ok(DescribeMetadataQuorumAdmission {
            observer: DescribeMetadataQuorumObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeMetadataQuorumOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DescribeMetadataQuorumHostError> {
    let transition = operation
        .machine
        .apply(DescribeMetadataQuorumInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeMetadataQuorumEffect::Submit {
            operation_id,
            deadline: core_deadline,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(DescribeMetadataQuorumHostError::SubmissionMismatch);
            }
            operation.submission = Some(DescribeMetadataQuorumSubmission {
                operation_id,
                deadline,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(DescribeMetadataQuorumEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DescribeMetadataQuorumHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeMetadataQuorumHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeMetadataQuorumAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeMetadataQuorumAdmissionErrorKind::Capacity,
        _ => DescribeMetadataQuorumAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge() -> Option<usize> {
    size_of::<DescribeMetadataQuorumOperation>()
        .checked_add(size_of::<DescribeMetadataQuorumSubmission>())
}
