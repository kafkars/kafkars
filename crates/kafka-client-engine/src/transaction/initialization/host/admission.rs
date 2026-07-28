//! Atomic terminal and retained-byte reservation before transaction acceptance.

use kafka_client_core::{
    Moment, TransactionInitializationEffect, TransactionInitializationInput,
    TransactionInitializationMachine, TransactionInitializationPlan,
};

use crate::completion::CompletionRegistryError;

use super::{
    TRANSACTION_INITIALIZATION_CAPACITY, TRANSACTION_INITIALIZATION_OPERATION_BYTES,
    TRANSACTION_INITIALIZATION_RETAINED_BYTES, TransactionInitializationAdmission,
    TransactionInitializationHost, TransactionInitializationOperation,
};
use crate::transaction::initialization::{
    TransactionInitializationAdmissionErrorKind, TransactionInitializationHostError,
    TransactionInitializationObserver, TransactionInitializationRequest,
    TransactionLifecycleControlPort,
};

impl TransactionInitializationHost {
    pub(in crate::transaction::initialization) fn try_admit(
        &mut self,
        now: Moment,
        deadline: crate::clock::OperationDeadline,
        request: TransactionInitializationRequest,
        plan: TransactionInitializationPlan,
        lifetime: std::sync::Arc<dyn Send + Sync>,
        control: TransactionLifecycleControlPort,
    ) -> Result<
        TransactionInitializationAdmission,
        (
            TransactionInitializationAdmissionErrorKind,
            TransactionInitializationRequest,
        ),
    > {
        let rejection = self.validate_admission();
        if let Some(kind) = rejection {
            return Err((kind, request));
        }
        let Some(operation_id) = self.next_operation_id else {
            return Err((
                TransactionInitializationAdmissionErrorKind::IdentityExhausted,
                request,
            ));
        };
        let Some(owner_id) = self.next_owner_id else {
            return Err((
                TransactionInitializationAdmissionErrorKind::IdentityExhausted,
                request,
            ));
        };
        let next_bytes = self
            .retained_bytes
            .checked_add(TRANSACTION_INITIALIZATION_OPERATION_BYTES)
            .filter(|bytes| *bytes <= TRANSACTION_INITIALIZATION_RETAINED_BYTES);
        let Some(next_bytes) = next_bytes else {
            return Err((
                TransactionInitializationAdmissionErrorKind::RetainedBytes,
                request,
            ));
        };
        let (completion_id, observer) = match self.completions.reserve() {
            Ok(reservation) => reservation,
            Err(error) => return Err((reservation_error(error), request)),
        };
        self.retained_bytes = next_bytes;
        self.next_operation_id = operation_id
            .get()
            .checked_add(1)
            .map(kafka_client_core::OperationId::from_raw);
        self.next_owner_id = owner_id
            .get()
            .checked_add(1)
            .map(kafka_client_core::TransactionalOwnerId::from_raw);
        let mut operation = TransactionInitializationOperation {
            operation_id,
            owner_id,
            machine: TransactionInitializationMachine::new(
                owner_id,
                operation_id,
                deadline.core(),
                plan,
            ),
            request: Some(request),
            completion_id,
            deadline,
            call: None,
            raw_terminal: None,
            terminal: None,
        };
        let transition = operation
            .machine
            .apply(owner_id, TransactionInitializationInput::Start { now });
        let mut fault = validate_start_transition(
            &mut operation,
            transition,
            owner_id,
            operation_id,
            deadline,
            plan,
        );
        self.operations.push(operation);
        if self
            .operations
            .last()
            .and_then(|operation| operation.terminal.as_ref())
            .is_some()
        {
            if let Err(error) = self.publish_terminal(self.operations.len() - 1) {
                fault.get_or_insert(error);
            }
        }
        if let Some(error) = fault {
            self.health = Some(error);
        }
        Ok(TransactionInitializationAdmission {
            observer: TransactionInitializationObserver::new(observer, lifetime, control),
            fault,
        })
    }

    fn validate_admission(&self) -> Option<TransactionInitializationAdmissionErrorKind> {
        if !self.accepting {
            return Some(TransactionInitializationAdmissionErrorKind::Closed);
        }
        if self.health.is_some() {
            return Some(TransactionInitializationAdmissionErrorKind::HostUnavailable);
        }
        if self.operations.len() + self.executions.len() >= TRANSACTION_INITIALIZATION_CAPACITY {
            return Some(TransactionInitializationAdmissionErrorKind::Capacity);
        }
        if self.next_operation_id.is_none() || self.next_owner_id.is_none() {
            return Some(TransactionInitializationAdmissionErrorKind::IdentityExhausted);
        }
        None
    }
}

fn validate_start_transition(
    operation: &mut TransactionInitializationOperation,
    transition: Result<
        kafka_client_core::TransactionInitializationTransition,
        kafka_client_core::TransactionInitializationMachineError,
    >,
    owner_id: kafka_client_core::TransactionalOwnerId,
    operation_id: kafka_client_core::OperationId,
    deadline: crate::clock::OperationDeadline,
    plan: TransactionInitializationPlan,
) -> Option<TransactionInitializationHostError> {
    match transition {
        Ok(transition) => match transition.into_effect() {
            Some(TransactionInitializationEffect::Submit {
                owner_id: effect_owner,
                operation_id: effect_operation,
                deadline: effect_deadline,
                plan: effect_plan,
            }) if effect_owner == owner_id
                && effect_operation == operation_id
                && effect_deadline == deadline.core()
                && effect_plan == plan =>
            {
                None
            }
            Some(TransactionInitializationEffect::Complete { terminal, .. }) => {
                operation.terminal =
                    crate::transaction::initialization::outcome::failed_retained_outcome(terminal);
                operation
                    .terminal
                    .is_none()
                    .then_some(TransactionInitializationHostError::UnexpectedEffect)
            }
            _ => Some(TransactionInitializationHostError::UnexpectedEffect),
        },
        Err(error) => Some(TransactionInitializationHostError::Machine(error)),
    }
}

const fn reservation_error(
    error: CompletionRegistryError,
) -> TransactionInitializationAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => TransactionInitializationAdmissionErrorKind::Capacity,
        _ => TransactionInitializationAdmissionErrorKind::HostUnavailable,
    }
}
