//! Concrete execution of the producer effects supported by the admission slice.

use kafka_client_core::{Moment, ProducerEffect, ProducerInput};

use super::{ProducerHost, ProducerHostInvariantError};

impl ProducerHost {
    pub(super) fn interpret_effect(
        &mut self,
        now: Moment,
        effect: ProducerEffect,
    ) -> Result<Option<ProducerInput>, ProducerHostInvariantError> {
        match effect {
            ProducerEffect::AccumulateExplicit {
                operation_id,
                batch_id,
                record,
                ..
            } => {
                let accumulator_bytes = self
                    .store
                    .accumulate(batch_id, operation_id, record.payload_id())
                    .map_err(ProducerHostInvariantError::Store)?;
                if accumulator_bytes != record.retained_bytes() {
                    return Err(ProducerHostInvariantError::CommittedFactsMismatch);
                }
                Ok(Some(ProducerInput::RecordAccumulated {
                    operation_id,
                    batch_id,
                    accumulator_bytes,
                    now,
                }))
            }
            ProducerEffect::ArmBatchTimer {
                batch_id,
                generation,
                deadline,
            } => {
                let _armed = self
                    .timers
                    .arm(batch_id, generation, deadline)
                    .map_err(ProducerHostInvariantError::Timer)?;
                Ok(None)
            }
            ProducerEffect::CancelBatchTimer {
                batch_id,
                generation,
            } => {
                let _cancelled = self.timers.cancel(batch_id, generation);
                Ok(None)
            }
            ProducerEffect::RemoveBatchMember {
                batch_id,
                operation_id,
            } => {
                let _payload_id = self
                    .store
                    .remove_member(batch_id, operation_id)
                    .map_err(ProducerHostInvariantError::Store)?;
                Ok(None)
            }
            ProducerEffect::ReleaseBatch { batch_id } => {
                self.cancel_pending_batch(batch_id);
                self.execution
                    .release_batch(&mut self.store, batch_id)
                    .map_err(ProducerHostInvariantError::Prepared)?;
                Ok(None)
            }
            ProducerEffect::ReleasePayload {
                payload_id,
                retained_bytes,
            } => {
                self.store
                    .release_payload(payload_id, retained_bytes)
                    .map_err(ProducerHostInvariantError::Store)?;
                Ok(None)
            }
            ProducerEffect::Complete {
                operation_id,
                completion,
            } => {
                let completion_id = self.bindings.completion(operation_id).ok_or(
                    ProducerHostInvariantError::Binding(
                        super::CompletionBindingError::UnknownOperation,
                    ),
                )?;
                if let Err((error, retained)) = self.completions.publish(completion_id, completion)
                {
                    self.retain_pending(ProducerEffect::Complete {
                        operation_id,
                        completion: retained,
                    })?;
                    if error == crate::completion::CompletionRegistryError::NotificationBackpressure
                    {
                        // Host limits keep notifier capacity at least as large as
                        // all completion slots, so this is unreachable while each
                        // slot publishes once. Retention still makes future host
                        // extensions retry-safe instead of losing terminal work.
                        return Ok(None);
                    }
                    return Err(ProducerHostInvariantError::Completion(error));
                }
                Ok(None)
            }
            pending @ (ProducerEffect::MaterializeBatch { .. }
            | ProducerEffect::SubmitProduce { .. }) => {
                self.retain_pending(pending)?;
                Ok(None)
            }
        }
    }

    fn retain_pending(&mut self, effect: ProducerEffect) -> Result<(), ProducerHostInvariantError> {
        if self.pending_effects.len() >= self.effect_capacity {
            return Err(ProducerHostInvariantError::PendingEffectCapacity);
        }
        self.pending_effects.push(effect);
        Ok(())
    }

    fn cancel_pending_batch(&mut self, batch_id: kafka_client_core::BatchId) {
        self.pending_effects.retain(|effect| {
            !matches!(
                effect,
                ProducerEffect::MaterializeBatch {
                    batch_id: pending,
                    ..
                } | ProducerEffect::SubmitProduce {
                    batch_id: pending,
                    ..
                } if *pending == batch_id
            )
        });
    }

    /// Retries bounded completion work without touching materialization or driver work.
    pub(crate) fn retry_pending_completions(
        &mut self,
        limit: usize,
    ) -> Result<usize, ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        let mut published = 0;
        let mut index = 0;
        while index < self.pending_effects.len() && published < limit {
            let ProducerEffect::Complete {
                operation_id,
                completion,
            } = self.pending_effects[index]
            else {
                index += 1;
                continue;
            };
            let Some(completion_id) = self.bindings.completion(operation_id) else {
                let error = ProducerHostInvariantError::Binding(
                    super::CompletionBindingError::UnknownOperation,
                );
                return Err(self.poison(error));
            };
            match self.completions.publish(completion_id, completion) {
                Ok(()) => {
                    let _effect = self.pending_effects.remove(index);
                    published += 1;
                }
                Err((crate::completion::CompletionRegistryError::NotificationBackpressure, _)) => {
                    break;
                }
                Err((error, _)) => {
                    return Err(self.poison(ProducerHostInvariantError::Completion(error)));
                }
            }
        }
        Ok(published)
    }
}
