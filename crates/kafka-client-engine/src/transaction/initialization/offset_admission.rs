//! Stable public rejection and exact input recovery for transactional offsets.

use core::fmt;

use crate::consumer::{GroupConsumerCheckpoint, GroupConsumerMetadata};

use super::TransactionOffsetCommitControlError;

/// Stable reason transactional offsets did not cross admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetsAdmissionErrorKind {
    /// The timeout could not form a positive absolute deadline.
    InvalidDeadline,
    /// Metadata and checkpoint do not name the same live assignment fence.
    StaleCheckpoint,
    /// Another caller currently owns the bounded transaction shard.
    Contended,
    /// Engine shutdown closed transactional offset admission.
    Closed,
    /// The initialized producer owner is no longer installed.
    StaleOwner,
    /// Another transactional offset transfer owns the fixed slot.
    Busy,
    /// Completion or retained-input capacity rejected the operation.
    Backpressure,
    /// The transaction lifecycle does not accept offset transfer.
    InvalidLifecycle,
    /// The metadata or checkpoint contains invalid broker input.
    InvalidInput,
    /// A nonreused operation identity was exhausted.
    IdentityExhausted,
}

/// Definitely-unsent rejection retaining the exact metadata and checkpoint.
#[must_use = "transactional offset rejection retains the original inputs"]
pub struct TransactionOffsetsAdmissionError {
    kind: TransactionOffsetsAdmissionErrorKind,
    metadata: GroupConsumerMetadata,
    checkpoint: GroupConsumerCheckpoint,
}

impl TransactionOffsetsAdmissionError {
    pub(super) const fn new(
        kind: TransactionOffsetsAdmissionErrorKind,
        metadata: GroupConsumerMetadata,
        checkpoint: GroupConsumerCheckpoint,
    ) -> Self {
        Self {
            kind,
            metadata,
            checkpoint,
        }
    }

    /// Returns the stable admission category.
    pub const fn kind(&self) -> TransactionOffsetsAdmissionErrorKind {
        self.kind
    }

    /// Recovers both exact caller inputs.
    pub fn into_parts(self) -> (GroupConsumerMetadata, GroupConsumerCheckpoint) {
        (self.metadata, self.checkpoint)
    }
}

impl fmt::Debug for TransactionOffsetsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionOffsetsAdmissionError")
            .field("kind", &self.kind)
            .field("metadata", &self.metadata)
            .field("checkpoint", &self.checkpoint)
            .finish()
    }
}

impl fmt::Display for TransactionOffsetsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transactional offsets rejected: {:?}", self.kind)
    }
}

impl std::error::Error for TransactionOffsetsAdmissionError {}

pub(super) const fn control_error_kind(
    error: &TransactionOffsetCommitControlError,
) -> TransactionOffsetsAdmissionErrorKind {
    use crate::transaction::offset_commit::TransactionOffsetCommitAdmissionErrorKind as Internal;
    match error.kind() {
        super::TransactionOffsetCommitControlErrorKind::Contended => {
            TransactionOffsetsAdmissionErrorKind::Contended
        }
        super::TransactionOffsetCommitControlErrorKind::Closed => {
            TransactionOffsetsAdmissionErrorKind::Closed
        }
        super::TransactionOffsetCommitControlErrorKind::Admission(kind) => match kind {
            Internal::Busy => TransactionOffsetsAdmissionErrorKind::Busy,
            Internal::CompletionCapacity
            | Internal::OffsetCount { .. }
            | Internal::RetainedBytes { .. } => TransactionOffsetsAdmissionErrorKind::Backpressure,
            Internal::StaleOwner => TransactionOffsetsAdmissionErrorKind::StaleOwner,
            Internal::InvalidLifecycle => TransactionOffsetsAdmissionErrorKind::InvalidLifecycle,
            Internal::InvalidInput => TransactionOffsetsAdmissionErrorKind::InvalidInput,
            Internal::IdentityExhausted => TransactionOffsetsAdmissionErrorKind::IdentityExhausted,
        },
    }
}
