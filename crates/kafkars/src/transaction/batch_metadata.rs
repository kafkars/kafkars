//! Stable acknowledgment metadata for one homogeneous transactional batch.

/// Kafka acknowledgment metadata shared by one accepted transactional batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionBatchMetadata {
    topic: String,
    partition: i32,
    base_offset: i64,
    last_offset: i64,
    record_count: usize,
    timestamp: Option<i64>,
    leader_epoch: Option<i32>,
}

impl TransactionBatchMetadata {
    pub(crate) const fn from_parts(
        topic: String,
        partition: i32,
        base_offset: i64,
        last_offset: i64,
        record_count: usize,
        timestamp: Option<i64>,
        leader_epoch: Option<i32>,
    ) -> Self {
        Self {
            topic,
            partition,
            base_offset,
            last_offset,
            record_count,
            timestamp,
            leader_epoch,
        }
    }

    /// Returns the exact canonical topic shared by every record.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the exact explicit partition shared by every record.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the acknowledged offset of the first record.
    pub const fn base_offset(&self) -> i64 {
        self.base_offset
    }

    /// Returns the acknowledged offset of the last record.
    pub const fn last_offset(&self) -> i64 {
        self.last_offset
    }

    /// Returns the exact nonzero record count admitted as one Produce batch.
    pub const fn record_count(&self) -> usize {
        self.record_count
    }

    /// Returns Kafka's batch append timestamp when supplied.
    pub const fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// Returns Kafka's leader epoch when supplied.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}
