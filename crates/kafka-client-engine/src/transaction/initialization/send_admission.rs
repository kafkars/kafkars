//! Stable public rejection categories for transactional record-send admission.

use core::fmt;

use crate::{
    producer::{
        ProducerSendCaptureError, ProducerSendCaptureErrorKind, PublicProducerRecord,
        TransactionRecordViewError,
    },
    transaction::{
        TransactionExecutionSendAdmissionErrorKind, send::TransactionSendAdmissionFailureKind,
    },
};

use super::{
    TransactionControlErrorKind, TransactionLifecycleControlError, TransactionSendControlError,
    TransactionSendControlErrorKind, control_error_mapping::control_error_kind,
};

/// Stable reason a transactional record did not cross send acceptance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionSendAdmissionErrorKind {
    /// The timeout could not form a positive absolute deadline.
    InvalidDeadline,
    /// The boundary Unix timestamp could not be represented.
    TimestampUnavailable,
    /// Kafka topic names cannot be empty.
    EmptyTopic,
    /// The explicit partition was negative.
    NegativeExplicitPartition,
    /// The public record's retained byte size overflowed.
    RetainedSizeOverflow,
    /// Another caller currently owns the bounded transaction shard.
    Contended,
    /// Engine shutdown has closed transactional send admission.
    Closed,
    /// The initialized transactional owner is no longer installed.
    StaleOwner,
    /// The record exceeded the configured retained-record byte limit.
    RetainedRecordBytes {
        /// Exact bytes retained by the rejected record.
        actual: usize,
        /// Configured retained-record byte limit.
        limit: usize,
    },
    /// The transaction's retained-topic count is full.
    RetainedTopicCapacity {
        /// Topic count required by the rejected send.
        actual: usize,
        /// Configured retained-topic count limit.
        limit: usize,
    },
    /// The transaction exceeded its retained-topic byte limit.
    RetainedTopicBytes {
        /// Topic bytes required by the rejected send.
        actual: usize,
        /// Configured retained-topic byte limit.
        limit: usize,
    },
    /// Retained-topic byte accounting overflowed.
    RetainedTopicBytesOverflow,
    /// The nonreused topic identity space was exhausted.
    TopicIdentityExhausted,
    /// Fixed send preparation could not reserve required memory.
    Allocation,
    /// Another send currently owns the transaction's fixed send slot.
    Busy,
    /// The nonreused send identity space was exhausted.
    SendIdentityExhausted,
    /// The resolved partition could not enter transactional sequencing.
    InvalidPartition,
    /// Transaction lifecycle state rejected this send.
    Transaction(TransactionControlErrorKind),
}

/// Rejected transactional send retaining the exact original record.
#[must_use = "transactional send rejection retains the original producer record"]
pub struct TransactionSendAdmissionError {
    kind: TransactionSendAdmissionErrorKind,
    record: PublicProducerRecord,
}

impl TransactionSendAdmissionError {
    pub(super) const fn new(
        kind: TransactionSendAdmissionErrorKind,
        record: PublicProducerRecord,
    ) -> Self {
        Self { kind, record }
    }

    /// Returns the stable admission rejection category.
    pub const fn kind(&self) -> TransactionSendAdmissionErrorKind {
        self.kind
    }

    /// Borrows the exact original record for inspection.
    pub const fn record(&self) -> &PublicProducerRecord {
        &self.record
    }

    /// Recovers the exact original record for retry or rerouting.
    pub fn into_record(self) -> PublicProducerRecord {
        self.record
    }
}

impl fmt::Debug for TransactionSendAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionSendAdmissionError")
            .field("kind", &self.kind)
            .field("record", &self.record)
            .finish()
    }
}

impl fmt::Display for TransactionSendAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transactional send rejected: {:?}", self.kind)
    }
}

impl std::error::Error for TransactionSendAdmissionError {}

pub(super) const fn capture_error_kind(
    error: ProducerSendCaptureError,
) -> TransactionSendAdmissionErrorKind {
    match error.kind() {
        ProducerSendCaptureErrorKind::DeadlineUnrepresentable => {
            TransactionSendAdmissionErrorKind::InvalidDeadline
        }
        ProducerSendCaptureErrorKind::TimestampUnrepresentable => {
            TransactionSendAdmissionErrorKind::TimestampUnavailable
        }
    }
}

pub(super) const fn record_error_kind(
    error: TransactionRecordViewError,
) -> TransactionSendAdmissionErrorKind {
    match error {
        TransactionRecordViewError::EmptyTopic => TransactionSendAdmissionErrorKind::EmptyTopic,
        TransactionRecordViewError::NegativeExplicitPartition => {
            TransactionSendAdmissionErrorKind::NegativeExplicitPartition
        }
        TransactionRecordViewError::RetainedSizeOverflow => {
            TransactionSendAdmissionErrorKind::RetainedSizeOverflow
        }
    }
}

pub(super) const fn control_send_error_kind(
    error: &TransactionSendControlError,
) -> TransactionSendAdmissionErrorKind {
    match error.kind() {
        TransactionSendControlErrorKind::Contended => TransactionSendAdmissionErrorKind::Contended,
        TransactionSendControlErrorKind::Closed => TransactionSendAdmissionErrorKind::Closed,
        TransactionSendControlErrorKind::Admission(kind) => execution_error_kind(kind),
    }
}

const fn execution_error_kind(
    error: TransactionExecutionSendAdmissionErrorKind,
) -> TransactionSendAdmissionErrorKind {
    match error {
        TransactionExecutionSendAdmissionErrorKind::StaleOwner => {
            TransactionSendAdmissionErrorKind::StaleOwner
        }
        TransactionExecutionSendAdmissionErrorKind::RetainedRecordBytes { actual, limit } => {
            TransactionSendAdmissionErrorKind::RetainedRecordBytes { actual, limit }
        }
        TransactionExecutionSendAdmissionErrorKind::RetainedTopicCapacity { actual, limit } => {
            TransactionSendAdmissionErrorKind::RetainedTopicCapacity { actual, limit }
        }
        TransactionExecutionSendAdmissionErrorKind::RetainedTopicBytes { actual, limit } => {
            TransactionSendAdmissionErrorKind::RetainedTopicBytes { actual, limit }
        }
        TransactionExecutionSendAdmissionErrorKind::RetainedTopicBytesOverflow => {
            TransactionSendAdmissionErrorKind::RetainedTopicBytesOverflow
        }
        TransactionExecutionSendAdmissionErrorKind::TopicIdentityExhausted => {
            TransactionSendAdmissionErrorKind::TopicIdentityExhausted
        }
        TransactionExecutionSendAdmissionErrorKind::Allocation => {
            TransactionSendAdmissionErrorKind::Allocation
        }
        TransactionExecutionSendAdmissionErrorKind::Send(kind) => send_error_kind(kind),
    }
}

const fn send_error_kind(
    error: TransactionSendAdmissionFailureKind,
) -> TransactionSendAdmissionErrorKind {
    match error {
        TransactionSendAdmissionFailureKind::Busy => TransactionSendAdmissionErrorKind::Busy,
        TransactionSendAdmissionFailureKind::SendIdentityExhausted => {
            TransactionSendAdmissionErrorKind::SendIdentityExhausted
        }
        TransactionSendAdmissionFailureKind::InvalidPartition => {
            TransactionSendAdmissionErrorKind::InvalidPartition
        }
        TransactionSendAdmissionFailureKind::Lifecycle(error) => {
            TransactionSendAdmissionErrorKind::Transaction(control_error_kind(
                TransactionLifecycleControlError::Host(error),
            ))
        }
    }
}
