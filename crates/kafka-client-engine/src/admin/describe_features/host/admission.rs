//! Atomic completion and per-result reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    DescribeFeaturesEffect, DescribeFeaturesInput, DescribeFeaturesMachine, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DESCRIBE_FEATURES_CAPACITY, DESCRIBE_FEATURES_RESULT_BYTES, DESCRIBE_FEATURES_RETAINED_BYTES,
    DescribeFeaturesAdmission, DescribeFeaturesHandoff, DescribeFeaturesHost,
    DescribeFeaturesHostError, DescribeFeaturesOperation, DescribeFeaturesSubmission,
};
use crate::admin::describe_features::{
    DescribeFeaturesAdmissionErrorKind, DescribeFeaturesObserver,
};

impl DescribeFeaturesHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<DescribeFeaturesAdmission, DescribeFeaturesAdmissionErrorKind> {
        if !self.accepting {
            return Err(DescribeFeaturesAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= DESCRIBE_FEATURES_CAPACITY {
            return Err(DescribeFeaturesAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DescribeFeaturesAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge =
            request_owner_charge().ok_or(DescribeFeaturesAdmissionErrorKind::RetainedBytes)?;
        let operation_bytes = owner_charge
            .checked_add(DESCRIBE_FEATURES_RESULT_BYTES)
            .ok_or(DescribeFeaturesAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(operation_bytes)
            .filter(|total| *total <= DESCRIBE_FEATURES_RETAINED_BYTES)
            .ok_or(DescribeFeaturesAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = DescribeFeaturesOperation {
            operation_id,
            machine: DescribeFeaturesMachine::new(operation_id, deadline.core()),
            completion_id,
            deadline,
            retained_bytes: operation_bytes,
            remaining_result_bytes: DESCRIBE_FEATURES_RESULT_BYTES,
            submission: None,
            handoff: DescribeFeaturesHandoff::Untouched,
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
        Ok(DescribeFeaturesAdmission {
            observer: DescribeFeaturesObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DescribeFeaturesOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DescribeFeaturesHostError> {
    let transition = operation
        .machine
        .apply(DescribeFeaturesInput::Start { now })?;
    match transition.into_effect() {
        Some(DescribeFeaturesEffect::Submit {
            operation_id,
            deadline: core_deadline,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(DescribeFeaturesHostError::SubmissionMismatch);
            }
            operation.submission = Some(DescribeFeaturesSubmission {
                operation_id,
                deadline,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(DescribeFeaturesEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(DescribeFeaturesHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DescribeFeaturesHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DescribeFeaturesAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DescribeFeaturesAdmissionErrorKind::Capacity,
        _ => DescribeFeaturesAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge() -> Option<usize> {
    size_of::<DescribeFeaturesOperation>().checked_add(size_of::<DescribeFeaturesSubmission>())
}
