//! Preflight, agreement, and infallible commit for sealed cancellation.

mod pending;
#[cfg(test)]
mod pending_test;

use kafka_client_core::{
    BatchExecutionId, OperationId, ProducerCancellationOutcome, ProducerEffect,
    ProducerMachineError, ProducerTransition,
};

use self::pending::PendingRevisionPlan;
use super::{ProducerHostCancelError, ProducerRevisionError};
use crate::producer::{
    ProducerHost, ProducerHostInvariantError,
    batch_store::{
        BatchCancellationPhase, BatchRevisionExpectation, BatchRevisionReplacement,
        EngineBatchRevisionPlan,
    },
    execution::PreparedRevisionPlan,
};

pub(super) struct SealedRevisionPlan {
    previous: BatchExecutionId,
    expected_replacement: BatchRevisionReplacement,
    removed_operation_id: OperationId,
    pending: PendingRevisionPlan,
    batch: EngineBatchRevisionPlan,
    prepared: PreparedRevisionPlan,
}

impl SealedRevisionPlan {
    pub(super) const fn generation_exhausted(&self) -> bool {
        matches!(
            self.expected_replacement,
            BatchRevisionReplacement::Exhausted
        )
    }

    fn validate_effect(
        &self,
        effect: Option<ProducerEffect>,
    ) -> Result<(), ProducerHostInvariantError> {
        let expected_replacement = match self.expected_replacement {
            BatchRevisionReplacement::Empty => None,
            BatchRevisionReplacement::Next(execution) => Some(execution),
            BatchRevisionReplacement::Exhausted => {
                return Err(ProducerHostInvariantError::Core(
                    ProducerMachineError::ExecutionGenerationExhausted,
                ));
            }
        };
        match effect {
            Some(ProducerEffect::ReviseBatchExecution {
                previous,
                replacement,
                removed_operation_id,
            }) if previous == self.previous
                && replacement == expected_replacement
                && removed_operation_id == self.removed_operation_id =>
            {
                Ok(())
            }
            Some(ProducerEffect::ReviseBatchExecution { previous, .. }) => {
                Err(ProducerHostInvariantError::Revision(
                    ProducerRevisionError::RevisionEffectMismatch {
                        expected: self.previous,
                        retained: previous,
                    },
                ))
            }
            _ => Err(ProducerHostInvariantError::Revision(
                ProducerRevisionError::MissingRevisionEffect(self.previous),
            )),
        }
    }

    fn validate_outcome(
        &self,
        outcome: ProducerCancellationOutcome,
    ) -> Result<(), ProducerHostInvariantError> {
        if outcome == ProducerCancellationOutcome::CancelledNotSent {
            Ok(())
        } else {
            Err(ProducerHostInvariantError::Revision(
                ProducerRevisionError::RevisionOutcomeMismatch(self.previous),
            ))
        }
    }

    fn commit(
        self,
        effects: &mut Vec<ProducerEffect>,
        prepared: &mut crate::producer::execution::PreparedExecution,
        compression: &mut crate::producer::compression::CompressionWorkers,
        store: &mut crate::producer::ProducerStore,
    ) {
        self.pending.commit(effects, compression, self.previous);
        prepared.commit_revision(self.prepared);
        store.commit_batch_revision(self.batch);
    }
}

impl ProducerHost {
    pub(super) fn interpret_cancellation_transition(
        &mut self,
        transition: ProducerTransition,
        preflight: Option<SealedRevisionPlan>,
        outcome: ProducerCancellationOutcome,
    ) -> Result<(), ProducerHostInvariantError> {
        let effects = transition.into_effects();
        let start = if let Some(preflight) = preflight {
            preflight.validate_outcome(outcome)?;
            preflight.validate_effect(effects.first().copied())?;
            preflight.commit(
                &mut self.pending_effects,
                &mut self.execution,
                &mut self.compression,
                &mut self.store,
            );
            1
        } else {
            if let Some(execution) = effects.iter().find_map(revision_execution) {
                return Err(ProducerHostInvariantError::Revision(
                    ProducerRevisionError::UnexpectedRevisionEffect(execution),
                ));
            }
            0
        };
        for index in start..effects.len() {
            if let Err(error) = self.interpret_time_free_effect(effects[index]) {
                self.retain_terminal_tail(&effects[index + 1..]);
                return Err(error);
            }
        }
        Ok(())
    }

    pub(super) fn preflight_cancellation(
        &mut self,
        removed_operation_id: OperationId,
    ) -> Result<Option<SealedRevisionPlan>, ProducerHostCancelError> {
        let phase = self
            .store
            .cancellation_phase(removed_operation_id)
            .map_err(|error| self.preflight_failed(ProducerHostInvariantError::Store(error)))?;
        let (previous, open_pending, explicit_pending) = match phase {
            None | Some(BatchCancellationPhase::Submitted) => return Ok(None),
            Some(BatchCancellationPhase::Sealed(previous)) => (previous, false, None),
            Some(BatchCancellationPhase::RetryWaiting(previous)) => {
                (previous, false, Some(PendingRevisionPlan::RetryWaiting))
            }
            Some(BatchCancellationPhase::Open(batch_id)) => {
                let Some(previous) = self.open_pending_execution(batch_id).map_err(|error| {
                    self.preflight_failed(ProducerHostInvariantError::Revision(error))
                })?
                else {
                    return Ok(None);
                };
                (previous, true, None)
            }
        };
        let pending = match explicit_pending {
            Some(pending) => pending,
            None => self.plan_pending_revision(previous).map_err(|error| {
                self.preflight_failed(ProducerHostInvariantError::Revision(error))
            })?,
        };
        if open_pending && !matches!(pending, PendingRevisionPlan::Materialize(_)) {
            return Err(self.preflight_failed(ProducerHostInvariantError::Revision(
                ProducerRevisionError::OpenBatchPendingSubmit(previous),
            )));
        }
        let batch_expectation = if open_pending {
            BatchRevisionExpectation::OpenForMaterialization
        } else {
            pending.batch_expectation()
        };
        let batch = self
            .store
            .plan_batch_revision(previous, removed_operation_id, batch_expectation)
            .map_err(|error| self.preflight_failed(ProducerHostInvariantError::Store(error)))?;
        let expected_replacement = batch.expected_replacement();
        let prepared = self
            .execution
            .plan_revision(previous, pending.prepared_expectation())
            .map_err(|error| self.preflight_failed(ProducerHostInvariantError::Prepared(error)))?;
        Ok(Some(SealedRevisionPlan {
            previous,
            expected_replacement,
            removed_operation_id,
            pending,
            batch,
            prepared,
        }))
    }

    fn preflight_failed(&mut self, error: ProducerHostInvariantError) -> ProducerHostCancelError {
        ProducerHostCancelError::Invariant(self.poison(error))
    }
}

const fn revision_execution(effect: &ProducerEffect) -> Option<BatchExecutionId> {
    match effect {
        ProducerEffect::ReviseBatchExecution { previous, .. } => Some(*previous),
        _ => None,
    }
}
