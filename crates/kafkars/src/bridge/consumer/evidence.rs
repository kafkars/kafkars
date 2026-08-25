//! Private translation of one immutable assigned-consumer Fetch evidence view.

use kafka_client_engine::AssignedConsumerFetchEvidence as EngineEvidence;

/// Generated-free facade bridge over exact engine Fetch evidence.
#[derive(Debug)]
pub(crate) struct AssignedConsumerFetchEvidence {
    inner: EngineEvidence,
}

impl AssignedConsumerFetchEvidence {
    pub(super) const fn from_engine(inner: EngineEvidence) -> Self {
        Self { inner }
    }

    pub(crate) fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub(crate) const fn topic_uuid(&self) -> [u8; 16] {
        self.inner.topic_uuid()
    }

    pub(crate) const fn partition(&self) -> i32 {
        self.inner.partition()
    }

    pub(crate) const fn requested_offset(&self) -> i64 {
        self.inner.requested_offset()
    }

    pub(crate) const fn next_offset(&self) -> i64 {
        self.inner.next_offset()
    }

    pub(crate) const fn log_start_offset(&self) -> Option<i64> {
        self.inner.log_start_offset()
    }

    pub(crate) const fn last_stable_offset(&self) -> Option<i64> {
        self.inner.last_stable_offset()
    }

    pub(crate) const fn high_watermark(&self) -> Option<i64> {
        self.inner.high_watermark()
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.inner.retained_bytes()
    }
}
