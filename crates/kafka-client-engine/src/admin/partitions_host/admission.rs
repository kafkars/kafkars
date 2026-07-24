//! Atomic reservation and start of one `CreatePartitions` operation.

use kafka_client_core::{
    CreatePartitionsEffect, CreatePartitionsInput, CreatePartitionsMachine, CreatePartitionsPlan,
    Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    CREATE_PARTITIONS_RETAINED_BYTES, CreatePartitionsAdmission, CreatePartitionsHost,
    CreatePartitionsHostError, CreatePartitionsOperation, CreatePartitionsSubmission,
};
use crate::admin::{CreatePartitionsAdmissionErrorKind, CreatePartitionsObserver};

impl CreatePartitionsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: CreatePartitionsPlan,
        retained_bytes: usize,
    ) -> Result<CreatePartitionsAdmission, CreatePartitionsAdmissionErrorKind> {
        if !self.accepting {
            return Err(CreatePartitionsAdmissionErrorKind::Closed);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(CreatePartitionsAdmissionErrorKind::IdentityExhausted)?;
        let Some(total_bytes) = self.retained_bytes.checked_add(retained_bytes) else {
            return Err(CreatePartitionsAdmissionErrorKind::RetainedBytes);
        };
        if total_bytes > CREATE_PARTITIONS_RETAINED_BYTES {
            return Err(CreatePartitionsAdmissionErrorKind::RetainedBytes);
        }
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;
        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = CreatePartitionsOperation {
            operation_id,
            machine: CreatePartitionsMachine::new(operation_id, deadline.core(), plan),
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
        Ok(CreatePartitionsAdmission {
            observer: CreatePartitionsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut CreatePartitionsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, CreatePartitionsHostError> {
    let transition = operation
        .machine
        .apply(CreatePartitionsInput::Start { now })?;
    match transition.into_effect() {
        Some(CreatePartitionsEffect::Submit {
            operation_id, plan, ..
        }) => {
            operation.submission = Some(CreatePartitionsSubmission {
                operation_id,
                deadline,
                plan,
                retained_bytes: operation.retained_bytes,
            });
            Ok(false)
        }
        Some(CreatePartitionsEffect::Complete { terminal, .. }) => {
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(CreatePartitionsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> CreatePartitionsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => CreatePartitionsAdmissionErrorKind::Capacity,
        _ => CreatePartitionsAdmissionErrorKind::HostUnavailable,
    }
}
