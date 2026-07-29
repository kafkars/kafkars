//! Atomic completion and broker-unregistration envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    Moment, OperationId, UnregisterBrokerEffect, UnregisterBrokerInput, UnregisterBrokerMachine,
    UnregisterBrokerPlan,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    UNREGISTER_BROKER_CAPACITY, UNREGISTER_BROKER_RESULT_BYTES, UNREGISTER_BROKER_RETAINED_BYTES,
    UnregisterBrokerAdmission, UnregisterBrokerHandoff, UnregisterBrokerHost,
    UnregisterBrokerHostError, UnregisterBrokerOperation, UnregisterBrokerSubmission,
};
use crate::admin::unregister_broker::{
    UnregisterBrokerAdmissionErrorKind, UnregisterBrokerObserver,
};

impl UnregisterBrokerHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: UnregisterBrokerPlan,
    ) -> Result<UnregisterBrokerAdmission, UnregisterBrokerAdmissionErrorKind> {
        if !self.accepting {
            return Err(UnregisterBrokerAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= UNREGISTER_BROKER_CAPACITY {
            return Err(UnregisterBrokerAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(UnregisterBrokerAdmissionErrorKind::IdentityExhausted)?;
        let operation_bytes = request_owner_charge()
            .and_then(|charge| charge.checked_add(UNREGISTER_BROKER_RESULT_BYTES))
            .ok_or(UnregisterBrokerAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(operation_bytes)
            .filter(|total| *total <= UNREGISTER_BROKER_RETAINED_BYTES)
            .ok_or(UnregisterBrokerAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = UnregisterBrokerOperation {
            operation_id,
            machine: UnregisterBrokerMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: operation_bytes,
            remaining_result_bytes: UNREGISTER_BROKER_RESULT_BYTES,
            submission: None,
            handoff: UnregisterBrokerHandoff::Untouched,
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
        if terminal_ready && let Err(error) = self.publish_terminal(self.operations.len() - 1) {
            self.health = Some(error);
            fault = Some(error);
        }
        Ok(UnregisterBrokerAdmission {
            observer: UnregisterBrokerObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut UnregisterBrokerOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, UnregisterBrokerHostError> {
    let transition = operation
        .machine
        .apply(UnregisterBrokerInput::Start { now })?;
    match transition.into_effect() {
        Some(UnregisterBrokerEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(UnregisterBrokerHostError::SubmissionMismatch);
            }
            operation.submission = Some(UnregisterBrokerSubmission {
                operation_id,
                deadline,
                plan,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(UnregisterBrokerEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(UnregisterBrokerHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(UnregisterBrokerHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> UnregisterBrokerAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => UnregisterBrokerAdmissionErrorKind::Capacity,
        _ => UnregisterBrokerAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge() -> Option<usize> {
    size_of::<UnregisterBrokerOperation>().checked_add(size_of::<UnregisterBrokerSubmission>())
}
