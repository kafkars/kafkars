//! Deterministic facts accepted by the explicit-partition producer machine.

use crate::{
    BatchExecutionId, BatchId, BatchTimerGeneration, ByteCount, Deadline, DeliveryStatus,
    ExplicitRecord, Moment, OperationId, ProducerAttemptFailureKind, ProducerBatchSuccess,
    ProducerBrokerFailure, ProducerIdentityGeneration,
};

/// One external fact applied at a time to producer policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerInput {
    /// Reports a broker-issued nontransactional identity.
    ProducerIdentityAcquired {
        /// Exact acquisition generation.
        generation: ProducerIdentityGeneration,
        /// Broker-issued producer ID.
        producer_id: i64,
        /// Broker-issued producer epoch.
        producer_epoch: i16,
        /// Monotonic observation when the terminal identity result was applied.
        now: Moment,
    },
    /// Reports terminal failure of one identity acquisition.
    ProducerIdentityFailed {
        /// Exact acquisition generation.
        generation: ProducerIdentityGeneration,
        /// Exact signed broker code, or no code for local/transport failure.
        broker_code: Option<core::num::NonZeroI16>,
        /// Monotonic observation when the broker failure was applied.
        now: Moment,
    },
    /// Reports that an exact producer-identity retry schedule became due.
    ProducerIdentityRetryDue {
        /// Schedule previously emitted by deterministic producer policy.
        schedule: crate::ProducerIdentityRetrySchedule,
        /// Monotonic observation used to enforce backoff and public deadlines.
        now: Moment,
    },
    /// Reports that the unchanged identity-request deadline elapsed.
    ProducerIdentityDeadlineElapsed {
        /// Exact acquisition generation.
        generation: ProducerIdentityGeneration,
        /// Monotonic observation used to classify each retained batch deadline.
        now: Moment,
    },
    /// Reports a non-deadline driver failure of one identity request.
    ProducerIdentityRequestFailed {
        /// Exact acquisition generation.
        generation: ProducerIdentityGeneration,
        /// Monotonic observation used to preserve already elapsed public deadlines.
        now: Moment,
    },
    /// Requests atomic admission of byte-free explicit record facts.
    AdmitExplicit {
        /// Current monotonic observation at the admission boundary.
        now: Moment,
        /// Absolute deadline captured at the public call boundary.
        deadline: Deadline,
        /// Opaque payload identity and validated explicit route.
        record: ExplicitRecord,
    },
    /// Reserves one accepted record before automatic partitioning can promote it.
    AdmitWaiting {
        /// Current monotonic observation at the admission boundary.
        now: Moment,
        /// Absolute deadline captured at the public call boundary.
        deadline: Deadline,
        /// Bytes retained by the independently bounded waiting owner.
        retained_bytes: ByteCount,
    },
    /// Transfers one exact waiting operation into active producer ownership.
    PromoteWaiting {
        /// Stable identity allocated when waiting ownership was accepted.
        operation_id: OperationId,
        /// Current monotonic observation at promotion.
        now: Moment,
        /// Opaque payload identity and selected explicit route.
        record: ExplicitRecord,
    },
    /// Reports terminal settlement before a waiting record was promoted.
    WaitingTerminal {
        /// Stable identity allocated when waiting ownership was accepted.
        operation_id: OperationId,
        /// Core-owned semantic reason the waiting record settled.
        terminal: crate::ProducerWaitingTerminal,
    },
    /// Requests cancellation of one accepted producer operation.
    CancelRequested {
        /// Operation whose current ownership stage decides the outcome.
        operation_id: OperationId,
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
        /// Current monotonic observation after rejection.
        now: Moment,
        /// Normalized structural reason the attempt was not admitted.
        failure: ProducerAttemptFailureKind,
    },
    /// Reports semantic broker success after driver ownership.
    BrokerSucceeded {
        /// Exact driver-owned execution that produced the response.
        execution: BatchExecutionId,
        /// Offset and optional broker metadata for per-record fan-out.
        success: ProducerBatchSuccess,
    },
    /// Reports a protocol-normalized broker failure fact after driver ownership.
    BrokerFailed {
        /// Exact driver-owned execution that produced the response.
        execution: BatchExecutionId,
        /// Monotonic observation after any required route refresh.
        now: Moment,
        /// Semantic broker category with its exact signed diagnostic code.
        failure: ProducerBrokerFailure,
        /// Driver-owned certainty for this request attempt.
        delivery: DeliveryStatus,
        /// Whether the exact failed route was fenced before retry policy runs.
        route_refreshed: bool,
    },
    /// Reports that a broker-terminal route refresh reached the original deadline.
    RouteRefreshDeadlineElapsed {
        /// Exact driver-owned execution whose routing failure is retained.
        execution: BatchExecutionId,
        /// Monotonic observation at or after the original public deadline.
        now: Moment,
        /// Driver-owned certainty already established by the failed attempt.
        delivery: DeliveryStatus,
    },
    /// Reports that the exact driver-owned request reached its public deadline.
    DriverDeadlineElapsed {
        /// Exact driver-owned execution whose request deadline elapsed.
        execution: BatchExecutionId,
        /// Monotonic observation at or after the original public deadline.
        now: Moment,
        /// Driver-owned certainty already established by the expired attempt.
        delivery: DeliveryStatus,
    },
    /// Reports transport failure after driver ownership.
    TransportFailed {
        /// Exact driver-owned execution that reached a terminal failure.
        execution: BatchExecutionId,
        /// Current monotonic observation at terminal normalization.
        now: Moment,
        /// Normalized structural reason independent of delivery certainty.
        failure: ProducerAttemptFailureKind,
        /// Driver-owned certainty for this request attempt.
        delivery: DeliveryStatus,
        /// Whether the exact failed partition route was fenced before retry policy runs.
        route_refreshed: bool,
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
