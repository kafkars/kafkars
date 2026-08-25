//! Stable Kafka acknowledgement metadata for one transactional record.

use std::sync::Arc;

use kafka_client_core::ProducerBatchSuccess;

/// Kafka acknowledgment metadata for one transactional record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionSendMetadata {
    topic: Arc<str>,
    topic_uuid: Option<[u8; 16]>,
    partition: i32,
    offset: i64,
    last_offset: i64,
    timestamp: Option<i64>,
    leader_epoch: Option<i32>,
}

impl TransactionSendMetadata {
    pub(super) fn from_success(
        success: ProducerBatchSuccess,
        last_offset: i64,
        topic: Arc<str>,
        topic_uuid: Option<[u8; 16]>,
        partition: i32,
    ) -> Self {
        Self {
            topic,
            topic_uuid,
            partition,
            offset: success.base_offset(),
            last_offset,
            timestamp: success.append_timestamp(),
            leader_epoch: success.leader_epoch(),
        }
    }

    /// Returns the exact canonical topic spelling admitted for this send.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the exact UUID proven before this name-routed Produce attempt.
    pub const fn topic_uuid(&self) -> Option<[u8; 16]> {
        self.topic_uuid
    }

    /// Returns the acknowledged zero-based partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the record's absolute Kafka offset.
    pub const fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the acknowledged offset of the last record in this send.
    pub const fn last_offset(&self) -> i64 {
        self.last_offset
    }

    /// Returns Kafka's append timestamp when supplied.
    pub const fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// Returns Kafka's leader epoch when supplied.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}
