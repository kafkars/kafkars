//! Stage-aware per-record cancellation before driver ownership.

use crate::{
    CompressionPolicy, OperationId, ProducerCancellationOutcome, ProducerCompletion,
    ProducerEffect, ProducerFailure, ProducerMachineError, ProducerOperationState,
    ProducerTransition, TransitionError,
};

use super::{BatchState, ProducerMachine, lifecycle::Settlement};

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
                self.cancel_sealed_member(batch_id, operation_id)
            }
            ProducerOperationState::WaitingForCapacity { .. } => Err(
                ProducerMachineError::Transition(TransitionError::InvalidState),
            ),
        }
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
            BatchState::Materializing | BatchState::AwaitingDriver
        ) {
            return Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ));
        }
        let revision = batch.plan_revision(batch_id, operation_id)?;
        let survivors = revision
            .members
            .iter()
            .map(|member| member.operation_id)
            .collect::<Vec<_>>();
        self.require_batch_execution_restart(&survivors, batch_id)?;
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
            effects.push(ProducerEffect::MaterializeBatch {
                execution,
                compression: CompressionPolicy::Uncompressed,
            });
        }
        Ok(resolved(
            ProducerCancellationOutcome::CancelledNotSent,
            effects,
        ))
    }

    fn cancel_retry_waiting_member(
        &mut self,
        batch_id: crate::BatchId,
        operation_id: OperationId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if batch.state != BatchState::RetryWaiting {
            return Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ));
        }
        let revision = batch.plan_retry_revision(batch_id, operation_id)?;
        let previous = revision.batch.previous;
        let replacement = revision.batch.replacement;
        let cancelled_generation = revision.previous_timer;
        let timer_generation = revision.replacement_timer;
        let timer_deadline = revision.timer_deadline;
        let failure = ProducerFailure::cancelled();
        let mut terminal =
            self.terminal_effects(&[operation_id], |_| ProducerCompletion::Failed(failure))?;
        self.settle_operations(&[operation_id], Settlement::Cancelled)?;

        if let Some(replacement) = replacement {
            let batch = self
                .batches
                .get_mut(&batch_id)
                .ok_or(ProducerMachineError::UnknownBatch)?;
            debug_assert_eq!(
                revision.batch.replacement,
                Some(replacement),
                "preflighted replacement changed before commit",
            );
            batch.commit_retry_revision(revision);
        } else {
            self.batches.remove(&batch_id);
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
        if let Some(generation) = timer_generation {
            effects.push(ProducerEffect::ArmBatchTimer {
                batch_id,
                generation,
                deadline: timer_deadline,
            });
        } else {
            effects.push(ProducerEffect::CancelBatchTimer {
                batch_id,
                generation: cancelled_generation,
            });
        }
        effects.append(&mut terminal);
        effects.extend(flush_effects);
        Ok(resolved(
            ProducerCancellationOutcome::CancelledNotSent,
            effects,
        ))
    }
}

fn resolved(
    outcome: ProducerCancellationOutcome,
    effects: Vec<ProducerEffect>,
) -> ProducerTransition {
    ProducerTransition::with_cancellation(outcome, effects)
}
