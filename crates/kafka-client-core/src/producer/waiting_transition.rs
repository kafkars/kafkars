//! Waiting-operation admission and terminal settlement under producer flush barriers.

use crate::{
    AdmissionRejection, ByteCount, CompletionLedgerError, Deadline, DeliveryStatus, Moment,
    OperationId, ProducerCompletion, ProducerEffect, ProducerFailure, ProducerMachineError,
    ProducerOperation, ProducerOperationState, ProducerTransition, ProducerWaitingTerminal,
    TransitionError,
};

use super::{ProducerMachine, lifecycle::Settlement};

impl ProducerMachine {
    pub(crate) fn admit_waiting(
        &mut self,
        now: Moment,
        deadline: Deadline,
        retained_bytes: ByteCount,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.admission_open {
            return Err(ProducerMachineError::Admission(AdmissionRejection::Closed));
        }
        if deadline.is_elapsed_at(now) {
            return Err(ProducerMachineError::Admission(
                AdmissionRejection::DeadlineElapsed,
            ));
        }
        let operation_id = self
            .next_operation_id
            .ok_or(ProducerMachineError::Admission(
                AdmissionRejection::IdentityExhausted,
            ))?;
        self.completions
            .reserve(operation_id)
            .map_err(waiting_completion_rejection)?;
        self.operations.insert(
            operation_id,
            ProducerOperation::new(operation_id, deadline, retained_bytes),
        );
        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        Ok(ProducerTransition::with_admission(operation_id, Vec::new()))
    }

    pub(crate) fn waiting_terminal(
        &mut self,
        operation_id: OperationId,
        terminal: ProducerWaitingTerminal,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let (settlement, failure) = match terminal {
            ProducerWaitingTerminal::Cancelled => {
                (Settlement::Cancelled, ProducerFailure::waiting_cancelled())
            }
            ProducerWaitingTerminal::DeadlineElapsed => (
                Settlement::Expired,
                ProducerFailure::waiting_deadline_elapsed(),
            ),
            ProducerWaitingTerminal::Closed => (
                Settlement::Failed(DeliveryStatus::NotSent),
                ProducerFailure::execution_unavailable(DeliveryStatus::NotSent),
            ),
            ProducerWaitingTerminal::MetadataUnavailable { broker_code } => (
                Settlement::Failed(DeliveryStatus::NotSent),
                ProducerFailure::metadata_unavailable(broker_code),
            ),
        };
        Ok(ProducerTransition::from_effects(
            self.settle_waiting_operation(operation_id, settlement, failure)?,
        ))
    }

    pub(super) fn settle_waiting_operation(
        &mut self,
        operation_id: OperationId,
        settlement: Settlement,
        failure: ProducerFailure,
    ) -> Result<Vec<ProducerEffect>, ProducerMachineError> {
        let operation = self
            .operations
            .get(&operation_id)
            .ok_or(ProducerMachineError::UnknownOperation)?;
        if !matches!(
            operation.state(),
            ProducerOperationState::WaitingForCapacity { .. }
        ) {
            return Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ));
        }
        self.settle_operations(&[operation_id], settlement)?;
        let mut effects = vec![ProducerEffect::Complete {
            operation_id,
            completion: ProducerCompletion::Failed(failure),
        }];
        effects.extend(self.settle_ready_flushes());
        Ok(effects)
    }
}

fn waiting_completion_rejection(error: CompletionLedgerError) -> ProducerMachineError {
    ProducerMachineError::Admission(match error {
        CompletionLedgerError::Full => AdmissionRejection::CompletionCapacity,
        CompletionLedgerError::DuplicateOperation
        | CompletionLedgerError::UnknownOperation
        | CompletionLedgerError::AlreadyCompleted
        | CompletionLedgerError::NotReady => AdmissionRejection::IdentityExhausted,
    })
}
