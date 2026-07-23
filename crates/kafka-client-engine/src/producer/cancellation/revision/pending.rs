//! Exact pending-effect ownership plans for pre-driver cancellation.

use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId, BatchId, ProducerEffect};

use super::super::ProducerRevisionError;
use crate::producer::{
    ProducerHost, batch_store::BatchRevisionExpectation, execution::PreparedRevisionExpectation,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PendingRevisionPlan {
    Materialize(usize),
    Submit(usize),
    Armed,
    RetryWaiting,
}

impl PendingRevisionPlan {
    pub(super) const fn batch_expectation(self) -> BatchRevisionExpectation {
        match self {
            Self::Materialize(_) => BatchRevisionExpectation::ReadyForMaterialization,
            Self::Submit(_) | Self::Armed => BatchRevisionExpectation::Materialized,
            Self::RetryWaiting => BatchRevisionExpectation::RetryWaiting,
        }
    }

    pub(super) const fn prepared_expectation(self) -> PreparedRevisionExpectation {
        match self {
            Self::Materialize(_) | Self::RetryWaiting => PreparedRevisionExpectation::Absent,
            Self::Submit(_) => PreparedRevisionExpectation::Unarmed,
            Self::Armed => PreparedRevisionExpectation::Armed,
        }
    }

    pub(super) fn commit(self, effects: &mut Vec<ProducerEffect>) {
        match self {
            Self::Materialize(index) | Self::Submit(index) => {
                let removed = effects.remove(index);
                debug_assert!(pending_execution(removed).is_some());
            }
            Self::Armed | Self::RetryWaiting => {}
        }
    }
}

impl ProducerHost {
    pub(super) fn open_pending_execution(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchExecutionId>, ProducerRevisionError> {
        let expected = BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial());
        let mut found = None;
        for effect in self.pending_effects.iter().copied() {
            let Some((retained, kind)) = pending_execution(effect) else {
                continue;
            };
            if retained.batch_id() != batch_id {
                continue;
            }
            if retained != expected {
                return Err(ProducerRevisionError::StalePendingExecution { expected, retained });
            }
            if matches!(kind, PendingKind::Submit) {
                return Err(ProducerRevisionError::OpenBatchPendingSubmit(retained));
            }
            if found.replace(retained).is_some() {
                return Err(ProducerRevisionError::DuplicatePendingExecution(retained));
            }
        }
        Ok(found)
    }

    pub(super) fn plan_pending_revision(
        &self,
        previous: BatchExecutionId,
    ) -> Result<PendingRevisionPlan, ProducerRevisionError> {
        let mut plan = PendingRevisionPlan::Armed;
        for (index, effect) in self.pending_effects.iter().copied().enumerate() {
            let Some((execution, kind)) = pending_execution(effect) else {
                continue;
            };
            if execution.batch_id() != previous.batch_id() {
                continue;
            }
            if execution != previous {
                return Err(ProducerRevisionError::StalePendingExecution {
                    expected: previous,
                    retained: execution,
                });
            }
            if plan != PendingRevisionPlan::Armed {
                return Err(ProducerRevisionError::DuplicatePendingExecution(previous));
            }
            plan = match kind {
                PendingKind::Materialize => PendingRevisionPlan::Materialize(index),
                PendingKind::Submit => PendingRevisionPlan::Submit(index),
            };
        }
        Ok(plan)
    }
}

#[derive(Clone, Copy)]
enum PendingKind {
    Materialize,
    Submit,
}

const fn pending_execution(effect: ProducerEffect) -> Option<(BatchExecutionId, PendingKind)> {
    match effect {
        ProducerEffect::MaterializeBatch { execution, .. } => {
            Some((execution, PendingKind::Materialize))
        }
        ProducerEffect::SubmitProduce { execution, .. } => Some((execution, PendingKind::Submit)),
        _ => None,
    }
}
