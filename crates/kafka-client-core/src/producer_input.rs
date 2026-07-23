//! Deterministic facts accepted by the explicit-partition producer machine.

use crate::{
    BatchExecutionId, BatchId, BatchTimerGeneration, ByteCount, Deadline, DeliveryStatus,
    ExplicitRecord, Moment, OperationId, ProducerBatchSuccess, ProducerBrokerFailure,
};

/// One external fact applied at a time to producer policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerInput {
    /// Requests atomic admission of byte-free explicit record facts.
    AdmitExplicit {
        /// Current monotonic observation at the admission boundary.
        now: Moment,
        /// Absolute deadline captured at the public call boundary.
        deadline: Deadline,
        /// Opaque payload identity and validated explicit route.
        record: ExplicitRecord,
    },
    /// Reports mechanism-owned accumulation and conservative sizing.
    RecordAccumulated {
        /// Operation whose bytes entered the accumulator.
        operation_id: OperationId,
        /// Core-selected logical batch.
        batch_id: BatchId,
        /// Conservative bytes charged toward readiness, not exact wire length.
        accumulator_bytes: ByteCount,
        /// Current monotonic observation after accumulation.
        now: Moment,
    },
    /// Reports one virtual or production timer wakeup.
    BatchTimerFired {
        /// Batch named by the timer mechanism.
        batch_id: BatchId,
        /// Generation captured when the timer was armed.
        generation: BatchTimerGeneration,
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports successful wire-records materialization before driver ownership.
    BatchMaterialized {
        /// Exact membership snapshot whose bytes are ready.
        execution: BatchExecutionId,
        /// Current monotonic observation before submission.
        now: Moment,
    },
    /// Reports semantic materialization failure before driver ownership.
    BatchMaterializationFailed {
        /// Exact membership snapshot that could not be materialized.
        execution: BatchExecutionId,
    },
    /// Reports that the driver accepted ownership of the batch request.
    DriverAccepted {
        /// Exact membership snapshot accepted by transport.
        execution: BatchExecutionId,
    },
    /// Reports definite rejection before the driver accepted ownership.
    DriverRejected {
        /// Exact membership snapshot rejected before transport.
        execution: BatchExecutionId,
    },
    /// Reports semantic broker success after driver ownership.
    BrokerSucceeded {
        /// Correlated core batch identity.
        batch_id: BatchId,
        /// Offset and optional broker metadata for per-record fan-out.
        success: ProducerBatchSuccess,
    },
    /// Reports a protocol-normalized broker failure fact after driver ownership.
    BrokerFailed {
        /// Correlated core batch identity.
        batch_id: BatchId,
        /// Semantic broker category with its exact signed diagnostic code.
        failure: ProducerBrokerFailure,
        /// Driver-owned certainty for this request attempt.
        delivery: DeliveryStatus,
    },
    /// Reports transport failure after driver ownership.
    TransportFailed {
        /// Correlated core batch identity.
        batch_id: BatchId,
        /// Driver-owned certainty for this request attempt.
        delivery: DeliveryStatus,
    },
    /// Reports that production execution is permanently unavailable.
    ///
    /// This fact closes admission and settles every accepted operation because
    /// no remaining mechanism can make progress.
    ExecutionUnavailable,
    /// Captures a barrier at the next record-admission sequence.
    FlushRequested,
    /// Atomically captures a drain barrier and permanently closes record admission.
    CloseRequested,
    /// Reports that the engine released one retained flush result.
    FlushCompletionReclaimed {
        /// Flush whose result and wakeup state were reclaimed.
        flush_id: crate::FlushId,
    },
    /// Reports a monotonic observation for pre-driver operation expiry.
    DeadlineElapsed {
        /// Operation checked for expiration.
        operation_id: OperationId,
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the engine released its retained terminal result.
    CompletionReclaimed {
        /// Operation whose result and wakeup state were reclaimed.
        operation_id: OperationId,
    },
}
