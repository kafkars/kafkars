//! Atomic slot, terminal, and retained-byte reservation before machine creation.

use kafka_client_core::{
    IncrementalAlterConfigsEffect, IncrementalAlterConfigsInput, IncrementalAlterConfigsMachine,
    IncrementalAlterConfigsPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    INCREMENTAL_ALTER_CONFIGS_CAPACITY, INCREMENTAL_ALTER_CONFIGS_RETAINED_BYTES,
    IncrementalAlterConfigsAdmission, IncrementalAlterConfigsHandoff, IncrementalAlterConfigsHost,
    IncrementalAlterConfigsHostError, IncrementalAlterConfigsOperation,
    IncrementalAlterConfigsSubmission,
};
use crate::admin::alter_configs::{
    IncrementalAlterConfigsAdmissionErrorKind, IncrementalAlterConfigsObserver,
    model::IncrementalAlterConfigsRetention,
};

impl IncrementalAlterConfigsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: IncrementalAlterConfigsPlan,
        retention: IncrementalAlterConfigsRetention,
    ) -> Result<IncrementalAlterConfigsAdmission, IncrementalAlterConfigsAdmissionErrorKind> {
        if !self.accepting {
            return Err(IncrementalAlterConfigsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= INCREMENTAL_ALTER_CONFIGS_CAPACITY {
            return Err(IncrementalAlterConfigsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(IncrementalAlterConfigsAdmissionErrorKind::IdentityExhausted)?;
        let Some(total_bytes) = self.retained_bytes.checked_add(retention.total()) else {
            return Err(IncrementalAlterConfigsAdmissionErrorKind::RetainedBytes);
        };
        if total_bytes > INCREMENTAL_ALTER_CONFIGS_RETAINED_BYTES {
            return Err(IncrementalAlterConfigsAdmissionErrorKind::RetainedBytes);
        }
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = IncrementalAlterConfigsOperation {
            operation_id,
            machine: IncrementalAlterConfigsMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: retention.total(),
            result_limit: retention.result_limit(),
            submission: None,
            handoff: IncrementalAlterConfigsHandoff::Untouched,
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
        Ok(IncrementalAlterConfigsAdmission {
            observer: IncrementalAlterConfigsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut IncrementalAlterConfigsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, IncrementalAlterConfigsHostError> {
    let transition = operation
        .machine
        .apply(IncrementalAlterConfigsInput::Start { now })?;
    match transition.into_effect() {
        Some(IncrementalAlterConfigsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            route,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(IncrementalAlterConfigsHostError::SubmissionMismatch);
            }
            let result_limit = super::super::model::incremental_alter_configs_result_limit(&plan)
                .filter(|limit| *limit <= operation.result_limit)
                .ok_or(IncrementalAlterConfigsHostError::ByteAccounting)?;
            operation.submission = Some(IncrementalAlterConfigsSubmission {
                operation_id,
                deadline,
                route,
                plan,
                result_limit,
            });
            Ok(false)
        }
        Some(IncrementalAlterConfigsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(IncrementalAlterConfigsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(IncrementalAlterConfigsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> IncrementalAlterConfigsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => IncrementalAlterConfigsAdmissionErrorKind::Capacity,
        _ => IncrementalAlterConfigsAdmissionErrorKind::HostUnavailable,
    }
}
