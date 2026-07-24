//! Cancellation of identity-waiting and definitely-unsent retry members.

use crate::{
    BatchId, OperationId, ProducerCancellationOutcome, ProducerCompletion, ProducerEffect,
    ProducerFailure, ProducerMachineError, ProducerTransition, TransitionError,
};

use super::{
    BatchState, ProducerMachine, cancellation::resolved,
    idempotence_transition::next_timer_generation, lifecycle::Settlement,
};

impl ProducerMachine {
    pub(super) fn cancel_identity_waiting_member(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
        revision: super::BatchRevision,
        previous_timer: crate::BatchTimerGeneration,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let replacement = revision.replacement;
        let timer_plan = if revision.members.is_empty() {
            None
        } else {
            let generation = next_timer_generation(previous_timer)?;
            let deadline = revision
                .members
                .iter()
                .map(|member| member.deadline)
                .min()
                .ok_or(ProducerMachineError::UnknownBatch)?;
            Some((generation, deadline))
        };
        let failure = ProducerFailure::cancelled();
        let mut terminal =
            self.terminal_effects(&[operation_id], |_| ProducerCompletion::Failed(failure))?;
        self.settle_operations(&[operation_id], Settlement::Cancelled)?;
        let mut effects = Vec::new();
        if let Some((generation, deadline)) = timer_plan {
            let batch = self
                .batches
                .get_mut(&batch_id)
                .ok_or(ProducerMachineError::UnknownBatch)?;
            batch.commit_identity_revision(revision, generation, deadline);
            effects.push(ProducerEffect::RemoveBatchMember {
                batch_id,
                operation_id,
            });
            effects.push(ProducerEffect::ArmBatchTimer {
                batch_id,
                generation,
                deadline,
            });
        } else {
            self.batches.remove(&batch_id);
            effects.push(ProducerEffect::CancelBatchTimer {
                batch_id,
                generation: previous_timer,
            });
            effects.push(ProducerEffect::ReleaseBatch { batch_id });
        }
        effects.append(&mut terminal);
        effects.extend(self.settle_ready_flushes());
        debug_assert_eq!(replacement.is_some(), timer_plan.is_some());
        Ok(resolved(
            ProducerCancellationOutcome::CancelledNotSent,
            effects,
        ))
    }

    pub(super) fn cancel_retry_waiting_member(
        &mut self,
        batch_id: BatchId,
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
        let sequence_release = replacement
            .is_none()
            .then(|| self.plan_sequence_not_sent(batch_id))
            .transpose()?;
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
