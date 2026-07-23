//! Ordered producer effects interpreted by the engine without hidden policy.

use crate::{
    AdmissionSequence, BatchExecutionId, BatchId, BatchTimerGeneration, ByteCount, Deadline,
    ExplicitRecord, FlushId, OperationId, PartitionIndex, PayloadId, ProducerCompletion, TopicId,
};

/// Maximum execution-stop mechanism effects emitted per live record.
pub const EXECUTION_STOP_EFFECTS_PER_RECORD: usize = 4;

/// Maximum execution-stop terminal effects emitted per retained flush.
pub const EXECUTION_STOP_EFFECTS_PER_FLUSH: usize = 1;

/// Computes the combined record-plus-flush execution-stop transition bound.
pub const fn execution_stop_effect_capacity(
    record_capacity: usize,
    flush_capacity: usize,
) -> Option<usize> {
    let Some(record_effects) = record_capacity.checked_mul(EXECUTION_STOP_EFFECTS_PER_RECORD)
    else {
        return None;
    };
    let Some(flush_effects) = flush_capacity.checked_mul(EXECUTION_STOP_EFFECTS_PER_FLUSH) else {
        return None;
    };
    record_effects.checked_add(flush_effects)
}

/// Computes the maximum effects emitted by any one public producer transition.
///
/// Execution stop owns the general `4R + F` fan-out. An immediately settled
/// flush emits both acceptance and completion even when `R` is zero.
pub const fn producer_transition_effect_capacity(
    record_capacity: usize,
    flush_capacity: usize,
) -> Option<usize> {
    let Some(execution_stop) = execution_stop_effect_capacity(record_capacity, flush_capacity)
    else {
        return None;
    };
    let immediate_flush = if flush_capacity == 0 { 0 } else { 2 };
    Some(if execution_stop > immediate_flush {
        execution_stop
    } else {
        immediate_flush
    })
}

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
        /// Exact immutable membership snapshot to encode.
        execution: BatchExecutionId,
        /// Required record-batch compression mode.
        compression: CompressionPolicy,
    },
    /// Revoke one exact sealed execution and replace its membership atomically.
    ReviseBatchExecution {
        /// Exact generation whose mechanism-owned resources must be discarded.
        previous: BatchExecutionId,
        /// Replacement generation, or `None` when no members survive.
        replacement: Option<BatchExecutionId>,
        /// Sole member removed from the immutable execution snapshot.
        removed_operation_id: OperationId,
    },
    /// Submit an engine-materialized batch through the driver adapter.
    SubmitProduce {
        /// Exact encoded membership snapshot to hand to the driver.
        execution: BatchExecutionId,
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
