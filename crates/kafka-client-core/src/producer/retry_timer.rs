//! Retry timer preflight and transition back to materialization.

use crate::{
    BatchId, BatchTimerGeneration, Moment, ProducerMachineError, ProducerTransition,
    TransitionError,
};

use super::{BatchState, ProducerMachine};

impl ProducerMachine {
    pub(crate) fn retry_timer_fired(
        &mut self,
        batch_id: BatchId,
        generation: BatchTimerGeneration,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if batch.state != BatchState::RetryWaiting || generation != batch.timer_generation {
            return Ok(ProducerTransition::none());
        }
        if !batch.timer_deadline.is_elapsed_at(now) {
            return Err(ProducerMachineError::Transition(
                TransitionError::DeadlineNotElapsed,
            ));
        }
        let deadline = batch
            .earliest_deadline()
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if deadline.is_elapsed_at(now) {
            return self.settle_batch_failed(batch_id, crate::ProducerFailure::deadline_elapsed());
        }
        let execution = batch
            .execution_id(batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let members = batch.member_ids();
        let route = batch.route;
        let identity = self
            .idempotence
            .identity()
            .ok_or(ProducerMachineError::ProducerIdentityFenced)?;
        let sequence = batch
            .sequence_lease()
            .ok_or(ProducerMachineError::ProducerIdentityFenced)?;
        self.idempotence.require_owned_lease(route, sequence)?;
        for operation_id in &members {
            self.operations
                .get(operation_id)
                .ok_or(ProducerMachineError::UnknownOperation)?
                .require_retry_waiting(batch_id)
                .map_err(ProducerMachineError::Transition)?;
        }
        for operation_id in &members {
            if let Some(operation) = self.operations.get_mut(operation_id) {
                operation.commit_retry_ready(batch_id);
            }
        }
        let batch = self
            .batches
            .get_mut(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        batch.state = BatchState::Materializing;
        Ok(ProducerTransition::from_effects(vec![
            super::idempotence_transition::materialize_effect(execution, identity, sequence),
        ]))
    }
}
