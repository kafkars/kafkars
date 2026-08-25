//! Explicit failures at producer byte, membership, and release boundaries.

use std::{error::Error, fmt};

use super::ProducerRecord;

/// Failure to mutate the bounded producer store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerStoreError {
    /// Every configured producer record slot is retained.
    RecordCapacity,
    /// Retaining the record would exceed the configured application-byte bound.
    ByteCapacity,
    /// Every configured producer batch slot is retained.
    BatchCapacity,
    /// A record's retained byte size cannot be represented.
    RetainedSizeOverflow,
    /// A record has more headers than Kafka's signed count can represent.
    HeaderCountOutOfRange,
    /// The monotonic payload identity space is exhausted.
    PayloadIdentityExhausted,
    /// The monotonic topic identity space is exhausted.
    TopicIdentityExhausted,
    /// One producer lifetime observed conflicting UUID expectations for a topic name.
    TopicIdentityMismatch,
    /// A retained payload referenced an unknown topic catalog entry.
    UnknownTopic,
    /// The payload identity is unknown, stale, or already released.
    UnknownPayload,
    /// A reservation or payload is not in the required lifecycle state.
    InvalidPayloadState,
    /// The operation already belongs to an engine batch.
    DuplicateOperation,
    /// The payload already belongs to an engine batch.
    DuplicatePayloadMembership,
    /// The batch identity is unknown or was already released.
    UnknownBatch,
    /// The requested operation does not belong to the named batch.
    UnknownBatchMember,
    /// Records disagree with the route already owned by the batch.
    BatchRouteMismatch,
    /// A batch has no records to materialize.
    EmptyBatch,
    /// A batch has more records than Kafka's producer sequence domain.
    BatchRecordCountOutOfRange,
    /// A batch was already taken for materialization.
    BatchAlreadyMaterialized,
    /// A mechanism named a non-current sealed-batch execution.
    StaleBatchExecution,
    /// The explicit partition cannot be represented by the Kafka protocol.
    PartitionOutOfRange,
    /// The release byte count disagrees with the originally retained count.
    RetainedSizeMismatch,
    /// A payload cannot be released while a batch still owns its membership.
    PayloadStillBatched,
}

impl fmt::Display for ProducerStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RecordCapacity => "producer record capacity is full",
            Self::ByteCapacity => "producer retained-byte capacity is full",
            Self::BatchCapacity => "producer batch capacity is full",
            Self::RetainedSizeOverflow => "producer retained byte size overflowed",
            Self::HeaderCountOutOfRange => {
                "producer record header count exceeds Kafka's signed domain"
            }
            Self::PayloadIdentityExhausted => "producer payload identity space is exhausted",
            Self::TopicIdentityExhausted => "producer topic identity space is exhausted",
            Self::TopicIdentityMismatch => {
                "producer topic UUID expectation conflicts with its retained identity"
            }
            Self::UnknownTopic => "producer topic identity is stale",
            Self::UnknownPayload => "producer payload identity is stale",
            Self::InvalidPayloadState => "producer payload is in the wrong lifecycle state",
            Self::DuplicateOperation => "producer operation already belongs to a batch",
            Self::DuplicatePayloadMembership => "producer payload already belongs to a batch",
            Self::UnknownBatch => "producer batch identity is stale",
            Self::UnknownBatchMember => "operation is not a member of the producer batch",
            Self::BatchRouteMismatch => "producer batch contains inconsistent routes",
            Self::EmptyBatch => "producer batch has no records",
            Self::BatchRecordCountOutOfRange => {
                "producer batch record count exceeds Kafka's sequence domain"
            }
            Self::BatchAlreadyMaterialized => "producer batch was already materialized",
            Self::StaleBatchExecution => "producer batch execution identity is stale",
            Self::PartitionOutOfRange => "producer partition exceeds the Kafka protocol range",
            Self::RetainedSizeMismatch => "producer release byte count does not match admission",
            Self::PayloadStillBatched => "producer payload still belongs to a batch",
        })
    }
}

impl Error for ProducerStoreError {}

/// Capacity rejection that returns the caller's record without changing it.
#[derive(Debug)]
pub(crate) struct ProducerAdmissionError {
    reason: ProducerStoreError,
    record: ProducerRecord,
}

impl ProducerAdmissionError {
    pub(super) const fn new(reason: ProducerStoreError, record: ProducerRecord) -> Self {
        Self { reason, record }
    }

    /// Returns the bounded-admission reason.
    pub(crate) const fn reason(&self) -> ProducerStoreError {
        self.reason
    }

    /// Returns the exact record whose ownership never crossed admission.
    pub(crate) fn into_record(self) -> ProducerRecord {
        self.record
    }
}

impl fmt::Display for ProducerAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(formatter)
    }
}

impl Error for ProducerAdmissionError {}
