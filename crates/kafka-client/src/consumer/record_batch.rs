//! Owned consumer records, batches, and assignment-fenced checkpoints.

use bytes::Bytes;

use crate::record::Header;

/// One consumed record whose bytes remain valid while its batch is retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecord {
    topic: String,
    partition: i32,
    offset: i64,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<Header>,
}

impl ConsumerRecord {
    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the record offset.
    pub const fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns nullable key bytes.
    pub fn key(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    /// Returns nullable value bytes.
    pub fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    /// Returns ordered duplicate-preserving headers.
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }
}

/// Assignment-fenced next offsets for processed records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub(super) group_id: String,
    pub(super) assignment_epoch: u64,
}

impl Checkpoint {
    /// Returns the assignment generation used to reject stale commits.
    pub const fn assignment_epoch(&self) -> u64 {
        self.assignment_epoch
    }
}

/// Owned batch that releases retained-byte capacity when dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    records: Vec<ConsumerRecord>,
    checkpoint: Checkpoint,
}

impl RecordBatch {
    /// Returns records in fetch order.
    pub fn records(&self) -> &[ConsumerRecord] {
        &self.records
    }

    /// Returns the generation-fenced next-offset checkpoint.
    pub fn checkpoint(&self) -> Checkpoint {
        self.checkpoint.clone()
    }
}
