//! Batch-scoped terminal fan-out with release-before-completion ordering.

use crate::{
    BatchId, OperationId, ProducerBatchSuccess, ProducerCompletion, ProducerEffect,
    ProducerFailure, ProducerMachineError, ProducerTransition, RecordMetadata,
};

use super::{BatchState, ProducerMachine, lifecycle::Settlement};

impl ProducerMachine {
    pub(crate) fn expire_open_members(
        &mut self,
        batch_id: BatchId,
        ids: &[OperationId],
        observed_linger: bool,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        self.settle_open_members(
            batch_id,
            ids,
            observed_linger,
            Settlement::Expired,
            ProducerFailure::deadline_elapsed(),
        )
    }

    pub(crate) fn settle_open_members(
        &mut self,
        batch_id: BatchId,
        ids: &[OperationId],
        observed_linger: bool,
        settlement: Settlement,
        failure: ProducerFailure,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let route = batch.route;
        let cancelled_generation = batch.timer_generation;
        let removal = batch.plan_remove_members(ids, observed_linger)?;
        let timer_update = removal.timer_update;
        let empties_batch = removal.members.is_empty();
        let mut effects = self.terminal_effects(ids, |_| ProducerCompletion::Failed(failure))?;
        self.settle_operations(ids, settlement)?;
        let flush_effects = self.settle_ready_flushes();
        if empties_batch {
            self.remove_open_batch_if_current(route, batch_id);
            self.batches.remove(&batch_id);
            effects.insert(
                0,
                ProducerEffect::CancelBatchTimer {
                    batch_id,
                    generation: cancelled_generation,
                },
            );
            effects.insert(1, ProducerEffect::ReleaseBatch { batch_id });
        } else {
            let batch = self
                .batches
                .get_mut(&batch_id)
                .ok_or(ProducerMachineError::UnknownBatch)?;
            batch.commit_remove_members(removal);
            if let Some((generation, deadline)) = timer_update {
                effects.insert(
                    0,
                    ProducerEffect::ArmBatchTimer {
                        batch_id,
                        generation,
                        deadline,
                    },
                );
            }
            let insertion = usize::from(timer_update.is_some());
            for (offset, operation_id) in ids.iter().copied().enumerate() {
                effects.insert(
                    insertion + offset,
                    ProducerEffect::RemoveBatchMember {
                        batch_id,
                        operation_id,
                    },
                );
            }
        }
        effects.extend(flush_effects);
        Ok(ProducerTransition::from_effects(effects))
    }

    pub(crate) fn settle_batch_failed(
        &mut self,
        batch_id: BatchId,
        failure: ProducerFailure,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let ids = batch.member_ids();
        let route = batch.route;
        let mut effects =
            self.batch_terminal_effects(batch_id, &ids, |_| ProducerCompletion::Failed(failure))?;
        self.settle_operations(&ids, Settlement::Failed(failure.delivery()))?;
        let flush_effects = self.settle_ready_flushes();
        self.remove_open_batch_if_current(route, batch_id);
        self.batches.remove(&batch_id);
        effects.extend(flush_effects);
        Ok(ProducerTransition::from_effects(effects))
    }

    pub(crate) fn settle_batch_succeeded(
        &mut self,
        batch_id: BatchId,
        success: ProducerBatchSuccess,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if batch.state != BatchState::Submitted {
            return Err(ProducerMachineError::Transition(
                crate::TransitionError::InvalidState,
            ));
        }
        let ids = batch.member_ids();
        let route = batch.route;
        let mut completions = Vec::with_capacity(ids.len());
        for (index, _) in ids.iter().enumerate() {
            let index = i64::try_from(index).map_err(|_| ProducerMachineError::OffsetOverflow)?;
            let offset = success
                .base_offset()
                .checked_add(index)
                .ok_or(ProducerMachineError::OffsetOverflow)?;
            completions.push(ProducerCompletion::Delivered(RecordMetadata::new(
                route.partition,
                offset,
                success.append_timestamp(),
                success.leader_epoch(),
            )));
        }
        let mut effects = self.terminal_effects_with(&ids, &completions)?;
        effects.insert(0, ProducerEffect::ReleaseBatch { batch_id });
        self.settle_operations(&ids, Settlement::Delivered)?;
        let flush_effects = self.settle_ready_flushes();
        self.batches.remove(&batch_id);
        effects.extend(flush_effects);
        Ok(ProducerTransition::from_effects(effects))
    }

    fn batch_terminal_effects(
        &self,
        batch_id: BatchId,
        ids: &[OperationId],
        completion: impl FnMut(OperationId) -> ProducerCompletion,
    ) -> Result<Vec<ProducerEffect>, ProducerMachineError> {
        let mut effects = self.terminal_effects(ids, completion)?;
        effects.insert(0, ProducerEffect::ReleaseBatch { batch_id });
        Ok(effects)
    }

    pub(crate) fn terminal_effects(
        &self,
        ids: &[OperationId],
        mut completion: impl FnMut(OperationId) -> ProducerCompletion,
    ) -> Result<Vec<ProducerEffect>, ProducerMachineError> {
        let mut effects = Vec::with_capacity(ids.len().saturating_mul(2));
        for id in ids {
            let record = self
                .record(*id)
                .ok_or(ProducerMachineError::UnknownOperation)?;
            effects.push(ProducerEffect::ReleasePayload {
                payload_id: record.payload_id(),
                retained_bytes: record.retained_bytes(),
            });
        }
        for id in ids {
            effects.push(ProducerEffect::Complete {
                operation_id: *id,
                completion: completion(*id),
            });
        }
        Ok(effects)
    }

    fn terminal_effects_with(
        &self,
        ids: &[OperationId],
        completions: &[ProducerCompletion],
    ) -> Result<Vec<ProducerEffect>, ProducerMachineError> {
        if ids.len() != completions.len() {
            return Err(ProducerMachineError::UnknownOperation);
        }
        let mut effects = Vec::with_capacity(ids.len().saturating_mul(2));
        for id in ids {
            let record = self
                .record(*id)
                .ok_or(ProducerMachineError::UnknownOperation)?;
            effects.push(ProducerEffect::ReleasePayload {
                payload_id: record.payload_id(),
                retained_bytes: record.retained_bytes(),
            });
        }
        for (id, completion) in ids.iter().zip(completions) {
            effects.push(ProducerEffect::Complete {
                operation_id: *id,
                completion: *completion,
            });
        }
        Ok(effects)
    }

    fn remove_open_batch_if_current(&mut self, route: super::BatchRoute, batch_id: BatchId) {
        if self.open_batches.get(&route) == Some(&batch_id) {
            self.open_batches.remove(&route);
        }
    }
}
