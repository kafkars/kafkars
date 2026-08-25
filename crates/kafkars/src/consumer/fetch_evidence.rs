//! Immutable public evidence retained by one direct-consumer Fetch lease.

use crate::{TopicUuid, bridge::consumer as bridge};

/// Broker-correlated topic identity, offset window, and retained-byte charge.
#[derive(Debug)]
pub struct ConsumerFetchEvidence {
    inner: bridge::AssignedConsumerFetchEvidence,
}

impl ConsumerFetchEvidence {
    pub(super) const fn from_bridge(inner: bridge::AssignedConsumerFetchEvidence) -> Self {
        Self { inner }
    }

    /// Returns the retained Kafka topic name.
    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    /// Returns the exact nonzero broker-issued topic UUID.
    pub fn topic_uuid(&self) -> TopicUuid {
        TopicUuid::try_from_bytes(self.inner.topic_uuid())
            .unwrap_or_else(|| unreachable!("public Fetch evidence is UUID-qualified"))
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.inner.partition()
    }

    /// Returns the offset requested by this exact Fetch.
    pub const fn requested_offset(&self) -> i64 {
        self.inner.requested_offset()
    }

    /// Returns the exclusive next offset after all complete broker progress.
    pub const fn next_offset(&self) -> i64 {
        self.inner.next_offset()
    }

    /// Returns the broker's log-start offset when supplied.
    pub const fn log_start_offset(&self) -> Option<i64> {
        self.inner.log_start_offset()
    }

    /// Returns the broker's last-stable offset when supplied.
    pub const fn last_stable_offset(&self) -> Option<i64> {
        self.inner.last_stable_offset()
    }

    /// Returns the broker's high watermark when supplied.
    pub const fn high_watermark(&self) -> Option<i64> {
        self.inner.high_watermark()
    }

    /// Returns the exact stable Fetch-output bytes held by this batch lease.
    pub const fn retained_bytes(&self) -> usize {
        self.inner.retained_bytes()
    }
}
