//! Stable public outcome for one homogeneous transactional batch send.

use super::{TransactionSendFailure, TransactionSendMetadata, TransactionSendOutcome};

/// Kafka acknowledgment metadata shared by one homogeneous transactional batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionBatchSendMetadata {
    first_record: TransactionSendMetadata,
    record_count: usize,
}

impl TransactionBatchSendMetadata {
    pub(super) const fn new(first_record: TransactionSendMetadata, record_count: usize) -> Self {
        Self {
            first_record,
            record_count,
        }
    }

    /// Returns the exact canonical topic shared by every admitted record.
    pub fn topic(&self) -> &str {
        self.first_record.topic()
    }

    /// Returns the exact explicit partition shared by every admitted record.
    pub const fn partition(&self) -> i32 {
        self.first_record.partition()
    }

    /// Returns the acknowledged offset of the first record.
    pub const fn base_offset(&self) -> i64 {
        self.first_record.offset()
    }

    /// Returns the acknowledged offset of the last record.
    pub const fn last_offset(&self) -> i64 {
        self.first_record.last_offset()
    }

    /// Returns the exact nonzero number of records admitted as one batch.
    pub const fn record_count(&self) -> usize {
        self.record_count
    }

    /// Returns Kafka's batch append timestamp when supplied.
    pub const fn timestamp(&self) -> Option<i64> {
        self.first_record.timestamp()
    }

    /// Returns Kafka's leader epoch when supplied.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.first_record.leader_epoch()
    }
}

/// Exactly one public terminal for an accepted homogeneous transactional batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionBatchSendOutcome {
    /// Kafka acknowledged the whole ordered batch.
    Succeeded(TransactionBatchSendMetadata),
    /// The whole batch failed with one authoritative certainty and consequence.
    Failed(TransactionSendFailure),
}

pub(super) fn batch_outcome(
    outcome: TransactionSendOutcome,
    record_count: usize,
) -> TransactionBatchSendOutcome {
    match outcome {
        TransactionSendOutcome::Succeeded(metadata) => TransactionBatchSendOutcome::Succeeded(
            TransactionBatchSendMetadata::new(metadata, record_count),
        ),
        TransactionSendOutcome::Failed(failure) => TransactionBatchSendOutcome::Failed(failure),
    }
}
