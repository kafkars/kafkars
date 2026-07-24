//! Accumulator confirmation and generation-fenced timer input handling.

use crate::{
    BatchId, BatchTimerGeneration, ByteCount, Moment, OperationId, ProducerMachineError,
    ProducerTransition, TransitionError,
};

use super::ProducerMachine;

impl ProducerMachine {
    pub(crate) fn record_accumulated(
        &mut self,
        operation_id: OperationId,
        batch_id: BatchId,
        accumulator_bytes: ByteCount,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let operation = self
            .operation(operation_id)
            .ok_or(ProducerMachineError::UnknownOperation)?;
        if operation.batch_id() != Some(batch_id) {
            return Err(ProducerMachineError::Transition(
                TransitionError::BatchMismatch,
            ));
        }
        let deadline = operation
            .deadline()
            .ok_or(ProducerMachineError::Transition(
                TransitionError::AlreadyCompleted,
            ))?;
        if deadline.is_elapsed_at(now) {
            return self.expire_open_members(batch_id, &[operation_id], false);
        }
        let accumulation = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?
            .plan_accumulation(operation_id, accumulator_bytes)?;
        let seal = if accumulation.readies_batch {
            Some(self.plan_seal(batch_id)?)
        } else {
            None
        };
        let batch = self.batches.get_mut(&batch_id);
        debug_assert!(batch.is_some());
        if let Some(batch) = batch {
            batch.commit_accumulation(accumulation, accumulator_bytes);
        }
        match seal {
            Some(plan) => self.commit_seal(plan),
            None => Ok(ProducerTransition::none()),
        }
    }

    pub(crate) fn batch_timer_fired(
        &mut self,
        batch_id: BatchId,
        generation: BatchTimerGeneration,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let Some(batch) = self.batches.get(&batch_id) else {
            return Ok(ProducerTransition::none());
        };
        if batch.state == super::BatchState::AwaitingIdentity {
            if generation != batch.timer_generation {
                return Ok(ProducerTransition::none());
            }
            if !batch.timer_deadline.is_elapsed_at(now) {
                return Err(ProducerMachineError::Transition(
                    TransitionError::DeadlineNotElapsed,
                ));
            }
            return super::idempotence_transition::settle_waiting_identity_expiry(self, batch_id);
        }
        if batch.state == super::BatchState::RetryWaiting {
            return self.retry_timer_fired(batch_id, generation, now);
        }
        let Some(observation) = batch.plan_timer_observation(generation, now)? else {
            return Ok(ProducerTransition::none());
        };
        let expired = batch
            .members
            .iter()
            .filter(|member| member.deadline.is_elapsed_at(now))
            .map(|member| member.operation_id)
            .collect::<Vec<_>>();
        let mut effects = Vec::new();
        if !expired.is_empty() {
            let transition =
                self.expire_open_members(batch_id, &expired, observation.linger_elapsed)?;
            effects.extend_from_slice(transition.effects());
        }
        if !self.batches.contains_key(&batch_id) {
            return Ok(ProducerTransition::from_effects(effects));
        }
        if expired.is_empty() {
            let timer_update = self
                .batches
                .get(&batch_id)
                .ok_or(ProducerMachineError::UnknownBatch)?
                .plan_timer_rearm(observation)?;
            let seal = if observation.readies_batch {
                Some(self.plan_seal(batch_id)?)
            } else {
                None
            };
            let batch = self.batches.get_mut(&batch_id);
            debug_assert!(batch.is_some());
            if let Some(batch) = batch {
                batch.commit_timer_observation(observation, timer_update);
            }
            if let Some(seal) = seal {
                effects.extend_from_slice(self.commit_seal(seal)?.effects());
                return Ok(ProducerTransition::from_effects(effects));
            }
            if let Some((generation, deadline)) = timer_update {
                effects.push(crate::ProducerEffect::ArmBatchTimer {
                    batch_id,
                    generation,
                    deadline,
                });
            }
        }
        let ready = self.seal_if_ready(batch_id)?;
        effects.extend_from_slice(ready.effects());
        Ok(ProducerTransition::from_effects(effects))
    }
}
