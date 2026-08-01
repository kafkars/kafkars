//! Stage-aware per-record cancellation before driver ownership.

use crate::{
    OperationId, ProducerCancellationOutcome, ProducerCompletion, ProducerEffect, ProducerFailure,
    ProducerMachineError, ProducerOperationState, ProducerTransition, TransitionError,
};

use super::{
    BatchState, ProducerMachine, lifecycle::Settlement, materialization::materialize_effect,
};

impl ProducerMachine {
    pub(crate) fn cancel_requested(
        &mut self,
        operation_id: OperationId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let Some(operation) = self.operation(operation_id) else {
            return Ok(resolved(
                ProducerCancellationOutcome::AlreadyTerminal,
                Vec::new(),
            ));
        };
        let state = operation.state();
        match state {
            ProducerOperationState::Completed => Ok(resolved(
                ProducerCancellationOutcome::AlreadyTerminal,
                Vec::new(),
            )),
            ProducerOperationState::Submitted { .. } => {
                Ok(resolved(ProducerCancellationOutcome::TooLate, Vec::new()))
            }
            ProducerOperationState::RetryWaiting { batch_id, .. } => {
                if self.retry_may_have_been_sent(batch_id)? {
                    return Ok(resolved(ProducerCancellationOutcome::TooLate, Vec::new()));
                }
                self.cancel_retry_waiting_member(batch_id, operation_id)
            }
            ProducerOperationState::Accumulating { batch_id, .. } => {
                let mut effects = self
                    .settle_open_members(
                        batch_id,
                        &[operation_id],
                        false,
                        Settlement::Cancelled,
                        ProducerFailure::cancelled(),
                    )?
                    .into_effects();
                if self.batches.contains_key(&batch_id) {
                    effects.extend(self.seal_if_ready(batch_id)?.into_effects());
                }
                Ok(resolved(
                    ProducerCancellationOutcome::CancelledNotSent,
                    effects,
                ))
            }
            ProducerOperationState::Materializing { batch_id, .. }
            | ProducerOperationState::AwaitingDriver { batch_id, .. } => {
                if self.retry_may_have_been_sent(batch_id)? {
                    return Ok(resolved(ProducerCancellationOutcome::TooLate, Vec::new()));
                }
                self.cancel_sealed_member(batch_id, operation_id)
            }
            ProducerOperationState::WaitingForCapacity { .. } => {
                let effects = self.settle_waiting_operation(
                    operation_id,
                    Settlement::Cancelled,
                    ProducerFailure::waiting_cancelled(),
                )?;
                Ok(resolved(
                    ProducerCancellationOutcome::CancelledNotSent,
                    effects,
                ))
            }
        }
    }

    fn retry_may_have_been_sent(
        &self,
        batch_id: crate::BatchId,
    ) -> Result<bool, ProducerMachineError> {
        self.batches
            .get(&batch_id)
            .map(|batch| batch.prior_delivery() == crate::DeliveryStatus::PossiblySent)
            .ok_or(ProducerMachineError::UnknownBatch)
    }

    fn cancel_sealed_member(
        &mut self,
        batch_id: crate::BatchId,
        operation_id: OperationId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if !matches!(
            batch.state,
            BatchState::AwaitingIdentity | BatchState::Materializing | BatchState::AwaitingDriver
        ) {
            return Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ));
        }
        let revision = batch.plan_revision(batch_id, operation_id)?;
        if batch.state == BatchState::AwaitingIdentity {
            return self.cancel_identity_waiting_member(
                batch_id,
                operation_id,
                revision,
                batch.timer_generation,
            );
        }
        let survivors = revision
            .members
            .iter()
            .map(|member| member.operation_id)
            .collect::<Vec<_>>();
        self.require_batch_execution_restart(&survivors, batch_id)?;
        let sequence_release = revision
            .replacement
            .is_none()
            .then(|| self.plan_sequence_not_sent(batch_id))
            .transpose()?;
        let failure = ProducerFailure::cancelled();
        let mut terminal =
            self.terminal_effects(&[operation_id], |_| ProducerCompletion::Failed(failure))?;
        self.settle_operations(&[operation_id], Settlement::Cancelled)?;
        self.commit_batch_execution_restart(&survivors, batch_id);

        let previous = revision.previous;
        let replacement = revision.replacement;
        if replacement.is_some() {
            let batch = self.batches.get_mut(&batch_id);
            debug_assert!(batch.is_some());
            if let Some(batch) = batch {
                batch.commit_revision(revision);
            }
        } else {
            self.batches.remove(&batch_id);
        }
        if let Some(sequence_release) = sequence_release {
            self.commit_sequence_not_sent(sequence_release);
        }
        let flush_effects = self.settle_ready_flushes();

        let mut effects = Vec::with_capacity(
            terminal
                .len()
                .saturating_add(flush_effects.len())
                .saturating_add(2),
        );
        effects.push(ProducerEffect::ReviseBatchExecution {
            previous,
            replacement,
            removed_operation_id: operation_id,
        });
        effects.append(&mut terminal);
        effects.extend(flush_effects);
        if let Some(execution) = replacement {
            let batch = self
                .batches
                .get(&batch_id)
                .ok_or(ProducerMachineError::UnknownBatch)?;
            let identity = self
                .idempotence
                .identity()
                .ok_or(ProducerMachineError::ProducerIdentityFenced)?;
            let sequence = batch
                .sequence_lease()
                .ok_or(ProducerMachineError::ProducerIdentityFenced)?;
            let (deadline_operation_id, deadline) = batch
                .earliest_deadline_owner()
                .ok_or(ProducerMachineError::UnknownBatch)?;
            effects.push(materialize_effect(
                execution,
                deadline_operation_id,
                deadline,
                self.compression,
                identity,
                sequence,
            ));
        }
        Ok(resolved(
            ProducerCancellationOutcome::CancelledNotSent,
            effects,
        ))
    }
}

pub(super) fn resolved(
    outcome: ProducerCancellationOutcome,
    effects: Vec<ProducerEffect>,
) -> ProducerTransition {
    ProducerTransition::with_cancellation(outcome, effects)
}
