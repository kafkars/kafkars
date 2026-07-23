//! Ordered producer effects interpreted by the engine without hidden policy.

use crate::{
    AdmissionSequence, BatchId, BatchTimerGeneration, ByteCount, Deadline, ExplicitRecord, FlushId,
    OperationId, PartitionIndex, PayloadId, ProducerCompletion, TopicId,
};

/// Fixed acknowledgment policy for the first producer vertical slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcknowledgementPolicy {
    /// Kafka acknowledges only after every in-sync replica accepts the batch.
    All,
}

/// Fixed compression policy for the first producer vertical slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionPolicy {
    /// The engine materializes a Kafka batch without compression.
    Uncompressed,
}

/// One mechanism request emitted by deterministic producer policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerEffect {
    /// Append engine-owned payload metadata to a core-selected batch.
    AccumulateExplicit {
        /// Stable public operation identity.
        operation_id: OperationId,
        /// Core-owned logical batch identity.
        batch_id: BatchId,
        /// Original absolute operation deadline.
        deadline: Deadline,
        /// Opaque payload identity and validated route facts.
        record: ExplicitRecord,
    },
    /// Arm or replace one generation-fenced batch timer.
    ArmBatchTimer {
        /// Batch whose policy wakeup is requested.
        batch_id: BatchId,
        /// Generation that must accompany the eventual timer fact.
        generation: BatchTimerGeneration,
        /// Absolute policy wakeup deadline.
        deadline: Deadline,
    },
    /// Cancel a timer generation after the batch seals or empties.
    CancelBatchTimer {
        /// Batch whose timer is no longer live.
        batch_id: BatchId,
        /// Exact generation being cancelled.
        generation: BatchTimerGeneration,
    },
    /// Materialize an already sealed accumulator through wire-records.
    MaterializeBatch {
        /// Core-owned logical batch identity.
        batch_id: BatchId,
        /// Required record-batch compression mode.
        compression: CompressionPolicy,
    },
    /// Submit an engine-materialized batch through the driver adapter.
    SubmitProduce {
        /// Core-owned logical batch identity.
        batch_id: BatchId,
        /// Live member the engine expires if driver ownership is not obtained.
        deadline_operation_id: OperationId,
        /// Earliest live member deadline handed to the driver unchanged.
        deadline: Deadline,
        /// Engine topic-catalog identity.
        topic_id: TopicId,
        /// Explicit Kafka partition shared by every member.
        partition: PartitionIndex,
        /// Required broker acknowledgment strength.
        acknowledgements: AcknowledgementPolicy,
    },
    /// Remove one expired record from a still-live engine accumulator.
    RemoveBatchMember {
        /// Batch that continues without the expired member.
        batch_id: BatchId,
        /// Operation whose record must not be materialized or submitted.
        operation_id: OperationId,
    },
    /// Release the engine accumulator or materialized bytes after settlement.
    ReleaseBatch {
        /// Engine batch no longer retained by policy.
        batch_id: BatchId,
    },
    /// Release engine-owned record bytes after terminal settlement.
    ReleasePayload {
        /// Engine payload no longer retained on behalf of the operation.
        payload_id: PayloadId,
        /// Bytes released from deterministic producer accounting.
        retained_bytes: ByteCount,
    },
    /// Publish one terminal result after every resource release effect.
    Complete {
        /// Stable public operation identity.
        operation_id: OperationId,
        /// Terminal result retained for its observer.
        completion: ProducerCompletion,
    },
    /// Commit a pre-reserved engine completion destination to a flush barrier.
    ///
    /// The engine must reserve bounded completion capacity before presenting
    /// the flush input. Interpreting this effect only binds that infallible
    /// reservation to the accepted identity.
    AcceptFlush {
        /// Stable flush identity.
        flush_id: FlushId,
        /// Next record-admission sequence captured at the flush call boundary.
        barrier: AdmissionSequence,
    },
    /// Request terminal flush publication after included terminal-decision effects.
    CompleteFlush {
        /// Stable flush identity.
        flush_id: FlushId,
    },
}

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
    ///
    /// The identity is derived from the admission effect without requiring an
    /// interpreter to depend on that effect's position in the ordered sequence.
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

    /// Transfers the ordered effects to their single engine interpreter.
    pub fn into_effects(self) -> Vec<ProducerEffect> {
        self.effects
    }
}
