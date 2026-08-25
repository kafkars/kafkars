//! Linear facade ownership over one bounded assigned-consumer delivery lease.

use crate::bridge::consumer::AssignedConsumerBatch;

use super::{ConsumerFetchEvidence, ConsumerRecords, OwnedConsumerBatch, OwnedConsumerRecords};

/// Owned batch whose borrowed records remain valid until this value is dropped.
///
/// Dropping the batch synchronously returns its exact retained-byte lease to
/// the assigned-consumer engine owner.
#[must_use = "dropping the batch releases its bounded delivery lease"]
pub struct RecordBatch {
    inner: AssignedConsumerBatch,
    evidence: ConsumerFetchEvidence,
}

impl RecordBatch {
    pub(crate) fn from_bridge(inner: AssignedConsumerBatch) -> Self {
        let evidence = ConsumerFetchEvidence::from_bridge(inner.evidence());
        Self { inner, evidence }
    }

    /// Returns the retained Kafka topic name.
    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    /// Returns the zero-based Kafka partition.
    pub fn partition(&self) -> i32 {
        self.inner.partition()
    }

    /// Returns the next offset after every record represented by this batch.
    pub fn checkpoint_next_offset(&self) -> i64 {
        self.inner.checkpoint_next_offset()
    }

    /// Returns immutable broker-correlated evidence for this exact Fetch lease.
    pub const fn evidence(&self) -> &ConsumerFetchEvidence {
        &self.evidence
    }

    /// Returns the number of normalized application records.
    pub fn len(&self) -> usize {
        self.inner.record_count()
    }

    /// Returns whether the batch contains no application records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterates normalized application records in Kafka order without copying.
    pub fn records(&self) -> ConsumerRecords<'_> {
        ConsumerRecords::from_bridge(self.inner.records())
    }

    /// Consumes the batch into non-clone records sharing its delivery lease.
    pub fn into_owned_records(self) -> OwnedConsumerRecords {
        OwnedConsumerRecords::from_bridge(self.inner.into_owned_records())
    }

    /// Consumes this delivery into an owned batch retaining its exact lease.
    pub fn into_owned(self) -> OwnedConsumerBatch {
        OwnedConsumerBatch::from_parts(self.inner.into_owned(), self.evidence)
    }
}

impl std::fmt::Debug for RecordBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RecordBatch")
            .field("topic", &self.topic())
            .field("partition", &self.partition())
            .field("checkpoint_next_offset", &self.checkpoint_next_offset())
            .field("evidence", &self.evidence())
            .field("len", &self.len())
            .finish()
    }
}
