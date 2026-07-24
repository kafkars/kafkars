//! Atomic reservation and deterministic start of one `DeleteTopics` operation.

use kafka_client_core::{
    DeleteTopicsEffect, DeleteTopicsInput, DeleteTopicsMachine, DeleteTopicsPlan, Moment,
    OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    DELETE_TOPICS_RETAINED_BYTES, DeleteTopicsAdmission, DeleteTopicsHost, DeleteTopicsHostError,
    DeleteTopicsOperation, DeleteTopicsSubmission,
};
use crate::admin::{DeleteTopicsAdmissionErrorKind, DeleteTopicsObserver};

impl DeleteTopicsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteTopicsPlan,
        retained_bytes: usize,
    ) -> Result<DeleteTopicsAdmission, DeleteTopicsAdmissionErrorKind> {
        if !self.accepting {
            return Err(DeleteTopicsAdmissionErrorKind::Closed);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(DeleteTopicsAdmissionErrorKind::IdentityExhausted)?;
        let Some(total_bytes) = self.retained_bytes.checked_add(retained_bytes) else {
            return Err(DeleteTopicsAdmissionErrorKind::RetainedBytes);
        };
        if total_bytes > DELETE_TOPICS_RETAINED_BYTES {
            return Err(DeleteTopicsAdmissionErrorKind::RetainedBytes);
        }
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;
        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = DeleteTopicsOperation {
            operation_id,
            machine: DeleteTopicsMachine::new(operation_id, deadline.core(), plan),
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
        Ok(DeleteTopicsAdmission {
            observer: DeleteTopicsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut DeleteTopicsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, DeleteTopicsHostError> {
    let transition = operation.machine.apply(DeleteTopicsInput::Start { now })?;
    match transition.into_effect() {
        Some(DeleteTopicsEffect::Submit {
            operation_id, plan, ..
        }) => {
            operation.submission = Some(DeleteTopicsSubmission {
                operation_id,
                deadline,
                plan,
                retained_bytes: operation.retained_bytes,
            });
            Ok(false)
        }
        Some(DeleteTopicsEffect::Complete { terminal, .. }) => {
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(DeleteTopicsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> DeleteTopicsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => DeleteTopicsAdmissionErrorKind::Capacity,
        _ => DeleteTopicsAdmissionErrorKind::HostUnavailable,
    }
}
