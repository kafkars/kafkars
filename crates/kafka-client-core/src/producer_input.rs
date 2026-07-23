//! Deterministic facts accepted by the explicit-partition producer machine.

use crate::{
    BatchId, BatchTimerGeneration, ByteCount, Deadline, DeliveryStatus, ExplicitRecord, Moment,
    OperationId, ProducerBatchSuccess,
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
        /// Batch whose bytes are ready for a Produce request.
        batch_id: BatchId,
        /// Current monotonic observation before submission.
        now: Moment,
    },
    /// Reports semantic materialization failure before driver ownership.
    BatchMaterializationFailed {
        /// Batch that could not be materialized.
        batch_id: BatchId,
    },
    /// Reports that the driver accepted ownership of the batch request.
    DriverAccepted {
        /// Correlated core batch identity.
        batch_id: BatchId,
    },
    /// Reports definite rejection before the driver accepted ownership.
    DriverRejected {
        /// Correlated core batch identity.
        batch_id: BatchId,
    },
    /// Reports semantic broker success after driver ownership.
    BrokerSucceeded {
        /// Correlated core batch identity.
        batch_id: BatchId,
        /// Offset and optional broker metadata for per-record fan-out.
        success: ProducerBatchSuccess,
    },
    /// Reports a raw broker failure fact after driver ownership.
    BrokerFailed {
        /// Correlated core batch identity.
        batch_id: BatchId,
        /// Exact Kafka error code extracted structurally from the response.
        broker_code: i16,
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
