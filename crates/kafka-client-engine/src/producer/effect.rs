//! Concrete execution of the producer effects supported by the admission slice.

use kafka_client_core::{Moment, ProducerEffect, ProducerInput};

use super::{ProducerHost, ProducerHostInvariantError};

impl ProducerHost {
    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive match keeps producer effect execution explicit"
    )]
    pub(super) fn interpret_effect_owned(
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
            effect => self.interpret_time_free_effect(effect).map(|()| None),
        }
    }

    pub(super) fn interpret_time_free_effect(
        &mut self,
        effect: ProducerEffect,
    ) -> Result<(), ProducerHostInvariantError> {
        match effect {
            pending @ ProducerEffect::AcquireProducerIdentity { .. } => {
                self.retain_pending(pending)?;
                Ok(())
            }
            pending @ ProducerEffect::ArmProducerIdentityRetry { .. } => {
                self.retain_pending(pending)?;
                Ok(())
            }
            ProducerEffect::AccumulateExplicit { .. } => {
                Err(ProducerHostInvariantError::UnexpectedCancellationEffect)
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
                Ok(())
            }
            ProducerEffect::CancelBatchTimer {
                batch_id,
                generation,
            } => {
                let _cancelled = self.timers.cancel(batch_id, generation);
                Ok(())
            }
            ProducerEffect::RemoveBatchMember {
                batch_id,
                operation_id,
            } => {
                let _payload_id = self
                    .store
                    .remove_member(batch_id, operation_id)
                    .map_err(ProducerHostInvariantError::Store)?;
                Ok(())
            }
            ProducerEffect::ReviseBatchExecution { previous, .. } => {
                Err(ProducerHostInvariantError::Revision(
                    super::cancellation::ProducerRevisionError::UnexpectedRevisionEffect(previous),
                ))
            }
            ProducerEffect::RetryBatchExecution {
                previous,
                replacement,
            } => self
                .store
                .start_batch_retry(previous, replacement)
                .map_err(ProducerHostInvariantError::Store),
            ProducerEffect::ReleaseBatch { batch_id } => {
                self.cancel_pending_batch(batch_id);
                self.compression.cancel_batch(batch_id);
                self.execution
                    .release_batch(&mut self.store, batch_id)
                    .map_err(ProducerHostInvariantError::Prepared)?;
                Ok(())
            }
            ProducerEffect::ReleasePayload {
                payload_id,
                retained_bytes,
            } => {
                self.store
                    .release_payload(payload_id, retained_bytes)
                    .map_err(ProducerHostInvariantError::Store)?;
                Ok(())
            }
            ProducerEffect::Complete {
                operation_id,
                completion,
            } => self.publish_or_retain_record_terminal(operation_id, completion),
            pending @ ProducerEffect::MaterializeBatch { execution, .. } => {
                self.store
                    .activate_batch_retry(execution)
                    .map_err(ProducerHostInvariantError::Store)?;
                self.retain_pending(pending)?;
                Ok(())
            }
            pending @ ProducerEffect::SubmitProduce { .. } => {
                self.retain_pending(pending)?;
                Ok(())
            }
            ProducerEffect::AcceptFlush { .. } => Ok(()),
            ProducerEffect::CompleteFlush { flush_id } => {
                self.publish_or_retain_flush_terminal(flush_id)
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
                    execution,
                    ..
                } | ProducerEffect::SubmitProduce {
                    execution,
                    ..
                } if execution.batch_id() == batch_id
            )
        });
    }
}
