//! Typed policy results pairing ordered effects with direct resolutions.

use crate::{FlushId, OperationId, ProducerEffect};

/// Dynamically sized ordered effects for batch fan-out.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProducerTransition {
    effects: Vec<ProducerEffect>,
}

impl ProducerTransition {
    pub(crate) const fn none() -> Self {
        Self {
            effects: Vec::new(),
        }
    }

    pub(crate) fn from_effects(effects: Vec<ProducerEffect>) -> Self {
        Self { effects }
    }

    /// Returns effects in the exact order the engine must interpret them.
    pub fn effects(&self) -> &[ProducerEffect] {
        &self.effects
    }

    /// Returns the operation accepted by an admission transition, when present.
    pub fn admitted_operation_id(&self) -> Option<OperationId> {
        self.effects.iter().find_map(|effect| match effect {
            ProducerEffect::AccumulateExplicit { operation_id, .. } => Some(*operation_id),
            ProducerEffect::ArmBatchTimer { .. }
            | ProducerEffect::CancelBatchTimer { .. }
            | ProducerEffect::MaterializeBatch { .. }
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
