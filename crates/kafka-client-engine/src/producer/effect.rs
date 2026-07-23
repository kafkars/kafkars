//! Concrete execution of the producer effects supported by the admission slice.

use kafka_client_core::{Moment, ProducerEffect, ProducerInput};

use super::{ProducerHost, ProducerHostInvariantError};

/// How far a failed mechanism effect progressed before returning its error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FailedEffectProgress {
    NotApplied,
    PartiallyApplied,
}

/// Exact ownership of the effect that returned an interpreter failure.
#[derive(Debug)]
pub(super) enum FailedEffectDisposition {
    Mechanism {
        effect: ProducerEffect,
        progress: FailedEffectProgress,
    },
    TerminalTransferred,
}

/// Error plus the non-duplicable disposition of its current effect.
#[derive(Debug)]
pub(super) struct EffectInterpretationFailure {
    error: ProducerHostInvariantError,
    disposition: FailedEffectDisposition,
}

impl EffectInterpretationFailure {
    fn mechanism(
        error: ProducerHostInvariantError,
        effect: ProducerEffect,
        progress: FailedEffectProgress,
    ) -> Self {
        Self {
            error,
            disposition: FailedEffectDisposition::Mechanism { effect, progress },
        }
    }

    fn terminal(error: ProducerHostInvariantError) -> Self {
        Self {
            error,
            disposition: FailedEffectDisposition::TerminalTransferred,
        }
    }

    pub(super) fn into_parts(self) -> (ProducerHostInvariantError, FailedEffectDisposition) {
        (self.error, self.disposition)
    }

    #[cfg(test)]
    pub(super) const fn error(&self) -> ProducerHostInvariantError {
        self.error
    }
}

impl ProducerHost {
    #[allow(
        clippy::result_large_err,
        reason = "the ownership-returning failure stores one Copy effect inline to avoid fault-path allocation"
    )]
    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive match must assign an ownership disposition to every effect"
    )]
    pub(super) fn interpret_effect_owned(
        &mut self,
        now: Moment,
        effect: ProducerEffect,
    ) -> Result<Option<ProducerInput>, EffectInterpretationFailure> {
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
                    .map_err(ProducerHostInvariantError::Store)
                    .map_err(|error| {
                        EffectInterpretationFailure::mechanism(
                            error,
                            effect,
                            FailedEffectProgress::NotApplied,
                        )
                    })?;
                if accumulator_bytes != record.retained_bytes() {
                    return Err(EffectInterpretationFailure::mechanism(
                        ProducerHostInvariantError::CommittedFactsMismatch,
                        effect,
                        FailedEffectProgress::PartiallyApplied,
                    ));
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
                    .map_err(ProducerHostInvariantError::Timer)
                    .map_err(|error| {
                        EffectInterpretationFailure::mechanism(
                            error,
                            effect,
                            FailedEffectProgress::NotApplied,
                        )
                    })?;
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
                    .map_err(ProducerHostInvariantError::Store)
                    .map_err(|error| {
                        EffectInterpretationFailure::mechanism(
                            error,
                            effect,
                            FailedEffectProgress::NotApplied,
                        )
                    })?;
                Ok(None)
            }
            ProducerEffect::ReleaseBatch { batch_id } => {
                self.cancel_pending_batch(batch_id);
                self.execution
                    .release_batch(&mut self.store, batch_id)
                    .map_err(ProducerHostInvariantError::Prepared)
                    .map_err(|error| {
                        EffectInterpretationFailure::mechanism(
                            error,
                            effect,
                            FailedEffectProgress::PartiallyApplied,
                        )
                    })?;
                Ok(None)
            }
            ProducerEffect::ReleasePayload {
                payload_id,
                retained_bytes,
            } => {
                self.store
                    .release_payload(payload_id, retained_bytes)
                    .map_err(ProducerHostInvariantError::Store)
                    .map_err(|error| {
                        EffectInterpretationFailure::mechanism(
                            error,
                            effect,
                            FailedEffectProgress::PartiallyApplied,
                        )
                    })?;
                Ok(None)
            }
            ProducerEffect::Complete {
                operation_id,
                completion,
            } => self
                .publish_or_retain_terminal(operation_id, completion)
                .map(|()| None)
                .map_err(EffectInterpretationFailure::terminal),
            pending @ (ProducerEffect::MaterializeBatch { .. }
            | ProducerEffect::SubmitProduce { .. }) => {
                self.retain_pending(pending).map_err(|error| {
                    EffectInterpretationFailure::mechanism(
                        error,
                        effect,
                        FailedEffectProgress::NotApplied,
                    )
                })?;
                Ok(None)
            }
            ProducerEffect::AcceptFlush { .. } | ProducerEffect::CompleteFlush { .. } => {
                Err(EffectInterpretationFailure::mechanism(
                    ProducerHostInvariantError::FlushControlUnavailable,
                    effect,
                    FailedEffectProgress::NotApplied,
                ))
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
