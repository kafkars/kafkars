//! Terminal outcomes and errors emitted by producer-operation transitions.

use core::fmt;

use crate::{ByteCount, PartitionIndex};

/// Core-owned resolution of one producer cancellation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerCancellationOutcome {
    /// Core cancelled the operation before driver ownership.
    CancelledNotSent,
    /// Driver ownership already made per-record cancellation unsafe.
    TooLate,
    /// The operation is terminal or no longer retained by core.
    AlreadyTerminal,
}

/// Certainty attached to a failed producer operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    /// The operation did not cross the transport ownership boundary.
    NotSent,
    /// The operation may have reached Kafka and a blind retry may duplicate it.
    PossiblySent,
}

/// Batch-level success facts decoded by the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatchSuccess {
    base_offset: i64,
    append_timestamp: Option<i64>,
    leader_epoch: Option<i32>,
}

impl ProducerBatchSuccess {
    /// Creates normalized broker acknowledgment facts.
    pub const fn new(
        base_offset: i64,
        append_timestamp: Option<i64>,
        leader_epoch: Option<i32>,
    ) -> Self {
        Self {
            base_offset,
            append_timestamp,
            leader_epoch,
        }
    }

    /// Returns the first absolute Kafka offset assigned to the batch.
    pub const fn base_offset(self) -> i64 {
        self.base_offset
    }

    /// Returns Kafka's append timestamp when supplied by the broker.
    pub const fn append_timestamp(self) -> Option<i64> {
        self.append_timestamp
    }

    /// Returns Kafka's current leader epoch when supplied by the broker.
    pub const fn leader_epoch(self) -> Option<i32> {
        self.leader_epoch
    }
}

/// Per-record delivery metadata derived from one acknowledged batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordMetadata {
    partition: PartitionIndex,
    offset: i64,
    append_timestamp: Option<i64>,
    leader_epoch: Option<i32>,
}

impl RecordMetadata {
    pub(crate) const fn new(
        partition: PartitionIndex,
        offset: i64,
        append_timestamp: Option<i64>,
        leader_epoch: Option<i32>,
    ) -> Self {
        Self {
            partition,
            offset,
            append_timestamp,
            leader_epoch,
        }
    }

    /// Returns the acknowledged partition.
    pub const fn partition(self) -> PartitionIndex {
        self.partition
    }

    /// Returns the record's absolute Kafka offset.
    pub const fn offset(self) -> i64 {
        self.offset
    }

    /// Returns Kafka's append timestamp when supplied by the broker.
    pub const fn append_timestamp(self) -> Option<i64> {
        self.append_timestamp
    }

    /// Returns the acknowledged leader epoch when supplied by the broker.
    pub const fn leader_epoch(self) -> Option<i32> {
        self.leader_epoch
    }
}

/// Terminal producer result retained for the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerCompletion {
    /// Kafka acknowledged the record batch containing the operation.
    Delivered(RecordMetadata),
    /// The operation failed with a normalized reason and delivery certainty.
    Failed(crate::ProducerFailure),
}

/// Resource-accounting fact emitted when an operation becomes terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalRelease {
    pub(crate) released_bytes: Option<ByteCount>,
}

impl TerminalRelease {
    /// Returns bytes released from the admitted producer budget, when any.
    pub const fn released_bytes(self) -> Option<ByteCount> {
        self.released_bytes
    }
}

/// Rejected producer-operation state transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionError {
    /// The requested action does not belong to the current lifecycle stage.
    InvalidState,
    /// The engine confirmed the same accumulator member more than once.
    AlreadyAccumulated,
    /// The engine reported a batch that does not own the operation.
    BatchMismatch,
    /// A claimed deadline expiration preceded the operation deadline.
    DeadlineNotElapsed,
    /// The operation already owns a terminal completion.
    AlreadyCompleted,
}

impl fmt::Display for TransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidState => formatter.write_str("invalid producer operation state"),
            Self::AlreadyAccumulated => {
                formatter.write_str("producer record was already accumulated")
            }
            Self::BatchMismatch => formatter.write_str("producer batch identity does not match"),
            Self::DeadlineNotElapsed => formatter.write_str("producer deadline has not elapsed"),
            Self::AlreadyCompleted => formatter.write_str("producer operation already completed"),
        }
    }
}

impl std::error::Error for TransitionError {}
