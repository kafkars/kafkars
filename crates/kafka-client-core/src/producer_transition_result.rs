//! Typed policy results pairing inline common effects with direct resolutions.

use core::fmt;

use crate::{FlushId, OperationId, ProducerCancellationOutcome, ProducerEffect};

/// Ordered effects and direct policy results, with one common effect retained inline.
#[derive(Clone, PartialEq, Eq)]
pub struct ProducerTransition {
    effects: ProducerEffects,
    admission: Option<OperationId>,
    cancellation: Option<ProducerCancellationOutcome>,
}

#[derive(Clone, PartialEq, Eq)]
enum ProducerEffects {
    None,
    One(ProducerEffect),
    Many(Vec<ProducerEffect>),
}

impl ProducerTransition {
    pub(crate) const fn none() -> Self {
        Self {
            effects: ProducerEffects::None,
            admission: None,
            cancellation: None,
        }
    }

    pub(crate) fn from_effects(effects: Vec<ProducerEffect>) -> Self {
        Self {
            effects: ProducerEffects::from_vec(effects),
            admission: None,
            cancellation: None,
        }
    }

    pub(crate) const fn from_effect(effect: ProducerEffect) -> Self {
        Self {
            effects: ProducerEffects::One(effect),
            admission: None,
            cancellation: None,
        }
    }

    pub(crate) fn with_admission(operation_id: OperationId, effects: Vec<ProducerEffect>) -> Self {
        Self {
            effects: ProducerEffects::from_vec(effects),
            admission: Some(operation_id),
            cancellation: None,
        }
    }

    pub(crate) fn with_cancellation(
        cancellation: ProducerCancellationOutcome,
        effects: Vec<ProducerEffect>,
    ) -> Self {
        Self {
            effects: ProducerEffects::from_vec(effects),
            admission: None,
            cancellation: Some(cancellation),
        }
    }

    /// Returns effects in the exact order the engine must interpret them.
    pub fn effects(&self) -> &[ProducerEffect] {
        self.effects.as_slice()
    }

    /// Returns the core-owned resolution for a cancellation input.
    pub const fn cancellation_outcome(&self) -> Option<ProducerCancellationOutcome> {
        self.cancellation
    }

    /// Returns the operation accepted by an admission transition, when present.
    pub fn admitted_operation_id(&self) -> Option<OperationId> {
        self.admission.or_else(|| {
            self.effects().iter().find_map(|effect| match effect {
                ProducerEffect::AccumulateExplicit { operation_id, .. } => Some(*operation_id),
                ProducerEffect::AcquireProducerIdentity { .. }
                | ProducerEffect::ArmProducerIdentityRetry { .. }
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
                | ProducerEffect::AcceptFlush { .. }
                | ProducerEffect::CompleteFlush { .. } => None,
            })
        })
    }

    /// Returns the flush accepted by a flush-request transition, when present.
    pub fn accepted_flush_id(&self) -> Option<FlushId> {
        self.effects().iter().find_map(|effect| match effect {
            ProducerEffect::AcceptFlush { flush_id, .. } => Some(*flush_id),
            ProducerEffect::AccumulateExplicit { .. }
            | ProducerEffect::AcquireProducerIdentity { .. }
            | ProducerEffect::ArmProducerIdentityRetry { .. }
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
        self.effects.into_vec()
    }

    #[cfg(test)]
    pub(crate) const fn effect_storage_is_inline(&self) -> bool {
        matches!(&self.effects, ProducerEffects::One(_))
    }
}

impl ProducerEffects {
    fn from_vec(effects: Vec<ProducerEffect>) -> Self {
        match effects.len() {
            0 => Self::None,
            1 => Self::One(effects[0]),
            _ => Self::Many(effects),
        }
    }

    fn as_slice(&self) -> &[ProducerEffect] {
        match self {
            Self::None => &[],
            Self::One(effect) => core::slice::from_ref(effect),
            Self::Many(effects) => effects,
        }
    }

    fn into_vec(self) -> Vec<ProducerEffect> {
        match self {
            Self::None => Vec::new(),
            Self::One(effect) => vec![effect],
            Self::Many(effects) => effects,
        }
    }
}

impl Default for ProducerTransition {
    fn default() -> Self {
        Self::none()
    }
}

impl fmt::Debug for ProducerTransition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerTransition")
            .field("effects", &self.effects())
            .field("admission", &self.admission)
            .field("cancellation", &self.cancellation)
            .finish()
    }
}
