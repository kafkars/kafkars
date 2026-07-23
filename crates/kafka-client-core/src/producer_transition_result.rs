//! Typed policy results pairing ordered effects with direct resolutions.

use crate::{FlushId, OperationId, ProducerCancellationOutcome, ProducerEffect};

/// Dynamically sized ordered effects and direct policy results.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProducerTransition {
    effects: Vec<ProducerEffect>,
    cancellation: Option<ProducerCancellationOutcome>,
}

impl ProducerTransition {
    pub(crate) const fn none() -> Self {
        Self {
            effects: Vec::new(),
            cancellation: None,
        }
    }

    pub(crate) fn from_effects(effects: Vec<ProducerEffect>) -> Self {
        Self {
            effects,
            cancellation: None,
        }
    }

    pub(crate) fn with_cancellation(
        cancellation: ProducerCancellationOutcome,
        effects: Vec<ProducerEffect>,
    ) -> Self {
        Self {
            effects,
            cancellation: Some(cancellation),
        }
    }

    /// Returns effects in the exact order the engine must interpret them.
    pub fn effects(&self) -> &[ProducerEffect] {
        &self.effects
    }

    /// Returns the core-owned resolution for a cancellation input.
    pub const fn cancellation_outcome(&self) -> Option<ProducerCancellationOutcome> {
        self.cancellation
    }

    /// Returns the operation accepted by an admission transition, when present.
    pub fn admitted_operation_id(&self) -> Option<OperationId> {
        self.effects.iter().find_map(|effect| match effect {
            ProducerEffect::AccumulateExplicit { operation_id, .. } => Some(*operation_id),
            ProducerEffect::ArmBatchTimer { .. }
            | ProducerEffect::CancelBatchTimer { .. }
            | ProducerEffect::MaterializeBatch { .. }
            | ProducerEffect::ReviseBatchExecution { .. }
            | ProducerEffect::RetryBatchExecution { .. }
            | ProducerEffect::SubmitProduce { .. }
            | ProducerEffect::RemoveBatchMember { .. }
            | ProducerEffect::ReleaseBatch { .. }
            | ProducerEffect::ReleasePayload { .. }
            | ProducerEffect::Complete { .. }
            | ProducerEffect::AcceptFlush { .. }
            | ProducerEffect::CompleteFlush { .. } => None,
        })
    }

    /// Returns the flush accepted by a flush-request transition, when present.
    pub fn accepted_flush_id(&self) -> Option<FlushId> {
        self.effects.iter().find_map(|effect| match effect {
            ProducerEffect::AcceptFlush { flush_id, .. } => Some(*flush_id),
            ProducerEffect::AccumulateExplicit { .. }
            | ProducerEffect::ArmBatchTimer { .. }
            | ProducerEffect::CancelBatchTimer { .. }
            | ProducerEffect::MaterializeBatch { .. }
            | ProducerEffect::ReviseBatchExecution { .. }
            | ProducerEffect::RetryBatchExecution { .. }
            | ProducerEffect::SubmitProduce { .. }
            | ProducerEffect::RemoveBatchMember { .. }
            | ProducerEffect::ReleaseBatch { .. }
            | ProducerEffect::ReleasePayload { .. }
            | ProducerEffect::Complete { .. }
            | ProducerEffect::CompleteFlush { .. } => None,
        })
    }

    /// Transfers the ordered effects to their single engine interpreter.
    pub fn into_effects(self) -> Vec<ProducerEffect> {
        self.effects
    }
}
