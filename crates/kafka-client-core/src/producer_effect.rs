//! Ordered producer effects interpreted by the engine without hidden policy.

use crate::{
    BatchId, ByteCount, Deadline, ExplicitRecord, OperationId, PartitionIndex, PayloadId,
    ProducerCompletion, TopicId,
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
    /// Append engine-owned payload metadata to its explicit partition accumulator.
    AccumulateExplicit {
        /// Stable public operation identity.
        operation_id: OperationId,
        /// Original absolute operation deadline.
        deadline: Deadline,
        /// Opaque payload identity and validated route facts.
        record: ExplicitRecord,
    },
    /// Submit an already materialized batch through the driver adapter.
    SubmitProduce {
        /// Operation represented by this first-slice batch.
        operation_id: OperationId,
        /// Engine-owned materialized batch identity.
        batch_id: BatchId,
        /// Original absolute deadline handed to the driver unchanged.
        deadline: Deadline,
        /// Engine topic-catalog identity.
        topic_id: TopicId,
        /// Explicit Kafka partition selected before admission.
        partition: PartitionIndex,
        /// Required broker acknowledgment strength.
        acknowledgements: AcknowledgementPolicy,
        /// Required record-batch compression mode.
        compression: CompressionPolicy,
    },
    /// Release an engine-materialized batch after terminal settlement.
    ReleaseBatch {
        /// Engine batch no longer retained on behalf of the operation.
        batch_id: BatchId,
    },
    /// Release engine-owned record bytes after terminal settlement.
    ReleasePayload {
        /// Engine payload no longer retained on behalf of the operation.
        payload_id: PayloadId,
        /// Bytes released from deterministic producer accounting.
        retained_bytes: ByteCount,
    },
    /// Publish the one terminal operation result after resource release effects.
    Complete {
        /// Stable public operation identity.
        operation_id: OperationId,
        /// Terminal result retained for its observer.
        completion: ProducerCompletion,
    },
}

/// Allocation-free ordered effect collection for one producer transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerTransition {
    /// The input changed ownership state without requesting engine work.
    None,
    /// One ordered effect.
    One([ProducerEffect; 1]),
    /// Two ordered effects.
    Two([ProducerEffect; 2]),
    /// Three ordered effects.
    Three([ProducerEffect; 3]),
}

impl ProducerTransition {
    /// Returns effects in the exact order the engine must interpret them.
    pub const fn effects(&self) -> &[ProducerEffect] {
        match self {
            Self::None => &[],
            Self::One(effects) => effects,
            Self::Two(effects) => effects,
            Self::Three(effects) => effects,
        }
    }
}
