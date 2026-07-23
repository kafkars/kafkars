//! Small value types describing unified prepared-entry ownership outcomes.

use std::{error::Error, fmt};

use kafka_client_core::BatchId;

/// Failure at prepared-request insertion, transfer, or release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedProduceError {
    /// Every configured prepared-request slot is occupied.
    BatchCapacity,
    /// Retaining the encoded records would exceed their configured byte bound.
    EncodedByteCapacity,
    /// Encoded byte accounting cannot be represented.
    EncodedByteOverflow,
    /// The logical batch already owns a prepared request.
    DuplicateBatch,
    /// Rollback attempted after core armed the prepared request.
    SubmissionArmed,
    /// The logical batch owns bytes from a different execution generation.
    ExecutionMismatch,
    /// The logical batch is unknown, already taken, or already released.
    UnknownBatch,
}

impl fmt::Display for PreparedProduceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BatchCapacity => "prepared Produce request capacity is full",
            Self::EncodedByteCapacity => "prepared Produce encoded-byte capacity is full",
            Self::EncodedByteOverflow => "prepared Produce encoded-byte accounting overflowed",
            Self::DuplicateBatch => "batch already owns a prepared Produce request",
            Self::SubmissionArmed => "prepared Produce request is already armed",
            Self::ExecutionMismatch => "prepared Produce execution identity is stale",
            Self::UnknownBatch => "prepared Produce batch identity is stale",
        })
    }
}

impl Error for PreparedProduceError {}

/// Current ownership of protocol-materialized requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedProduceStats {
    /// Number of retained prepared batches.
    pub(crate) batches: usize,
    /// Encoded record bytes retained across those batches.
    pub(crate) encoded_record_bytes: usize,
}

/// Failure to retain core-declared submission facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SubmissionDeadlineError {
    /// The batch already owns different core-declared deadline facts.
    ConflictingBatch {
        /// Batch whose second arm disagreed with the first.
        batch_id: BatchId,
    },
}

impl fmt::Display for SubmissionDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConflictingBatch { batch_id } => write!(
                formatter,
                "batch {} already owns different submission facts",
                batch_id.get()
            ),
        }
    }
}

impl Error for SubmissionDeadlineError {}
