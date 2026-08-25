//! Sole policy owner for authorized producer replacement executions.

use crate::{
    BatchExecutionId, BatchTimerGeneration, DeliveryStatus, Moment, ProducerAttemptFailureKind,
    ProducerEffect, ProducerFailure, ProducerMachineError, ProducerTransition, TransitionError,
};

use super::{BatchState, ProducerMachine};

impl ProducerMachine {
    pub(crate) fn driver_rejected(
        &mut self,
        execution: BatchExecutionId,
        now: Moment,
        failure: ProducerAttemptFailureKind,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let terminal = if failure == ProducerAttemptFailureKind::Identity {
            ProducerFailure::topic_identity_mismatch()
        } else {
            ProducerFailure::driver_rejected()
        };
        self.attempt_failed(
            execution,
            now,
            failure,
            DeliveryStatus::NotSent,
            BatchState::AwaitingDriver,
            terminal,
        )
    }

    pub(crate) fn transport_failed(
        &mut self,
        execution: BatchExecutionId,
        now: Moment,
        failure: ProducerAttemptFailureKind,
        delivery: DeliveryStatus,
        route_refreshed: bool,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if self.execution_is_current(execution)
            && delivery == DeliveryStatus::PossiblySent
            && route_refreshed
            && failure.is_structurally_transient()
        {
            let batch_id = execution.batch_id();
            self.require_batch_state(batch_id, BatchState::Submitted)?;
            if !self.idempotence.is_fenced() && self.retry_available(batch_id)? {
                let deadline = self
                    .batches
                    .get(&batch_id)
                    .and_then(super::ProducerBatch::earliest_deadline)
                    .ok_or(ProducerMachineError::UnknownBatch)?;
                if deadline.is_elapsed_at(now) {
                    return self
                        .settle_retry_terminal(batch_id, ProducerFailure::deadline_elapsed());
                }
                return self.start_retry(execution, now, deadline, BatchState::Submitted, delivery);
            }
        }
        if self.execution_is_current(execution) && delivery == DeliveryStatus::PossiblySent {
            return self.settle_uncertain_delivery(
                execution.batch_id(),
                ProducerFailure::attempt(failure, delivery),
            );
        }
        let transition = self.attempt_failed(
            execution,
            now,
            failure,
            delivery,
            BatchState::Submitted,
            ProducerFailure::attempt(failure, delivery),
        )?;
        Ok(transition)
    }

    fn attempt_failed(
        &mut self,
        execution: BatchExecutionId,
        now: Moment,
        failure: ProducerAttemptFailureKind,
        delivery: DeliveryStatus,
        expected: BatchState,
        terminal: ProducerFailure,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.execution_is_current(execution) {
            return Ok(ProducerTransition::none());
        }
        let batch_id = execution.batch_id();
        self.require_batch_state(batch_id, expected)?;
        if self.idempotence.is_fenced()
            || delivery != DeliveryStatus::NotSent
            || !failure.is_structurally_transient()
            || !self.retry_available(batch_id)?
        {
            return self.settle_retry_terminal(batch_id, terminal);
        }
        let deadline = self
            .batches
            .get(&batch_id)
            .and_then(super::ProducerBatch::earliest_deadline)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if deadline.is_elapsed_at(now) {
            return self.settle_retry_terminal(batch_id, ProducerFailure::deadline_elapsed());
        }
        self.start_retry(execution, now, deadline, expected, delivery)
    }

    pub(crate) fn retry_available(
        &self,
        batch_id: crate::BatchId,
    ) -> Result<bool, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        Ok(batch.retries_started < self.retry_policy.max_retries())
    }

    pub(crate) fn start_retry(
        &mut self,
        previous: BatchExecutionId,
        now: Moment,
        operation_deadline: crate::Deadline,
        expected: BatchState,
        delivery: DeliveryStatus,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch_id = previous.batch_id();
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let replacement_generation = previous
            .generation()
            .checked_next()
            .ok_or(ProducerMachineError::ExecutionGenerationExhausted)?;
        let timer_generation = batch
            .timer_generation
            .get()
            .checked_add(1)
            .map(BatchTimerGeneration::from_raw)
            .ok_or(ProducerMachineError::TimerGenerationExhausted)?;
        let retries_started =
            batch
                .retries_started
                .checked_add(1)
                .ok_or(ProducerMachineError::Transition(
                    TransitionError::InvalidState,
                ))?;
        let retry_deadline = now
            .checked_deadline_after(self.retry_policy.backoff_ticks())
            .map_or(operation_deadline, |backoff| {
                backoff.min(operation_deadline)
            });
        let members = batch.member_ids();
        self.require_retry_source(&members, batch_id, expected)?;

        for operation_id in &members {
            if let Some(operation) = self.operations.get_mut(operation_id) {
                operation.commit_retry_waiting(batch_id);
            }
        }
        let replacement = BatchExecutionId::new(batch_id, replacement_generation);
        let batch = self
            .batches
            .get_mut(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        batch.commit_retry_waiting(
            replacement_generation,
            retries_started,
            timer_generation,
            retry_deadline,
            delivery,
        );

        Ok(ProducerTransition::from_effects(vec![
            ProducerEffect::RetryBatchExecution {
                previous,
                replacement,
            },
            ProducerEffect::ArmBatchTimer {
                batch_id,
                generation: timer_generation,
                deadline: retry_deadline,
            },
        ]))
    }

    pub(crate) fn settle_retry_terminal(
        &mut self,
        batch_id: crate::BatchId,
        failure: ProducerFailure,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let prior_delivery = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?
            .prior_delivery();
        if prior_delivery == DeliveryStatus::PossiblySent {
            return self.settle_uncertain_delivery(
                batch_id,
                failure.with_delivery(DeliveryStatus::PossiblySent),
            );
        }
        self.settle_batch_failed(batch_id, failure)
    }

    fn require_retry_source(
        &self,
        members: &[crate::OperationId],
        batch_id: crate::BatchId,
        expected: BatchState,
    ) -> Result<(), ProducerMachineError> {
        for operation_id in members {
            let operation = self
                .operations
                .get(operation_id)
                .ok_or(ProducerMachineError::UnknownOperation)?;
            match expected {
                BatchState::AwaitingDriver => operation.require_awaiting_driver(batch_id),
                BatchState::Submitted => operation.require_submitted(batch_id),
                BatchState::Open
                | BatchState::AwaitingIdentity
                | BatchState::Materializing
                | BatchState::RetryWaiting => Err(TransitionError::InvalidState),
            }
            .map_err(ProducerMachineError::Transition)?;
        }
        Ok(())
    }
}
