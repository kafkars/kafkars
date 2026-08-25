//! Immutable public evidence for one UUID-qualified direct Fetch delivery.

use std::sync::Arc;

use super::AssignedConsumerDelivery;

/// Correlated topic identity, offset window, and exact retained-byte charge.
#[derive(Debug)]
pub struct AssignedConsumerFetchEvidence {
    topic: Arc<str>,
    topic_uuid: [u8; 16],
    partition: i32,
    requested_offset: i64,
    next_offset: i64,
    log_start_offset: Option<i64>,
    last_stable_offset: Option<i64>,
    high_watermark: Option<i64>,
    retained_bytes: usize,
}

impl AssignedConsumerFetchEvidence {
    pub(super) fn from_delivery(delivery: &AssignedConsumerDelivery) -> Self {
        let fetch = delivery
            .lease()
            .outcome()
            .outcome()
            .evidence()
            .unwrap_or_else(|| unreachable!("authorized delivery is a successful Fetch"));
        Self {
            topic: delivery.retained_topic(),
            topic_uuid: delivery.topic_uuid(),
            partition: delivery.partition(),
            requested_offset: fetch.requested_offset(),
            next_offset: fetch.next_offset(),
            log_start_offset: fetch.log_start_offset(),
            last_stable_offset: fetch.last_stable_offset(),
            high_watermark: fetch.high_watermark(),
            retained_bytes: delivery.lease().outcome().retained_bytes(),
        }
    }

    /// Returns the retained Kafka topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the exact nonzero broker-issued topic UUID bytes.
    pub const fn topic_uuid(&self) -> [u8; 16] {
        self.topic_uuid
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the Fetch request's starting offset.
    pub const fn requested_offset(&self) -> i64 {
        self.requested_offset
    }

    /// Returns the exclusive next offset after complete broker progress.
    pub const fn next_offset(&self) -> i64 {
        self.next_offset
    }

    /// Returns the broker's log-start offset when supplied.
    pub const fn log_start_offset(&self) -> Option<i64> {
        self.log_start_offset
    }

    /// Returns the broker's last-stable offset when supplied.
    pub const fn last_stable_offset(&self) -> Option<i64> {
        self.last_stable_offset
    }

    /// Returns the broker's high watermark when supplied.
    pub const fn high_watermark(&self) -> Option<i64> {
        self.high_watermark
    }

    /// Returns the exact stable Fetch-output charge held by this lease.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}
