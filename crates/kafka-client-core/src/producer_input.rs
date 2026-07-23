//! Deterministic facts accepted by the explicit-partition producer machine.

use crate::{BatchId, Deadline, DeliveryStatus, ExplicitRecord, Moment, OperationId};

/// One external fact applied at a time to producer policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerInput {
    /// Requests atomic admission of bytes-free explicit record facts.
    AdmitExplicit {
        /// Current monotonic observation at the admission boundary.
        now: Moment,
        /// Absolute deadline captured at the public call boundary.
        deadline: Deadline,
        /// Opaque payload identity and validated explicit route.
        record: ExplicitRecord,
    },
    /// Reports that the engine materialized an uncompressed batch.
    BatchReady {
        /// Operation whose record belongs to this first-slice batch.
        operation_id: OperationId,
        /// Engine-owned batch identity.
        batch_id: BatchId,
        /// Current monotonic observation before driver submission.
        now: Moment,
    },
    /// Reports that the driver accepted ownership of the batch request.
    DriverAccepted {
        /// Operation whose batch crossed the ownership boundary.
        operation_id: OperationId,
        /// Correlated engine batch identity.
        batch_id: BatchId,
    },
    /// Reports definite rejection before the driver accepted ownership.
    DriverRejected {
        /// Operation whose submission was rejected.
        operation_id: OperationId,
        /// Correlated engine batch identity.
        batch_id: BatchId,
    },
    /// Reports semantic broker success after driver ownership.
    BrokerSucceeded {
        /// Operation acknowledged by Kafka.
        operation_id: OperationId,
        /// Correlated engine batch identity.
        batch_id: BatchId,
    },
    /// Reports semantic broker or transport failure after driver ownership.
    BrokerFailed {
        /// Operation settled by the driver.
        operation_id: OperationId,
        /// Correlated engine batch identity.
        batch_id: BatchId,
        /// Driver-owned certainty for the failed attempt.
        delivery: DeliveryStatus,
    },
    /// Reports a monotonic observation for pre-driver deadline settlement.
    DeadlineElapsed {
        /// Operation checked for expiration.
        operation_id: OperationId,
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the engine released its retained terminal result.
    CompletionReclaimed {
        /// Operation whose engine-owned result and wakeup state were reclaimed.
        operation_id: OperationId,
    },
}
