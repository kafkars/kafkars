//! Private facade translation over one exact classic-group checkpoint.

use kafka_client_engine::GroupConsumerCheckpoint as EngineCheckpoint;

/// Linear bridge retaining the engine's exact assignment-fenced checkpoint.
pub(crate) struct GroupConsumerCheckpoint {
    inner: EngineCheckpoint,
}

impl GroupConsumerCheckpoint {
    pub(crate) const fn from_engine(inner: EngineCheckpoint) -> Self {
        Self { inner }
    }

    pub(crate) fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub(crate) fn partition(&self) -> i32 {
        self.inner.partition()
    }

    pub(crate) fn next_offset(&self) -> i64 {
        self.inner.next_offset()
    }

    pub(crate) fn into_engine(self) -> EngineCheckpoint {
        self.inner
    }
}

impl std::fmt::Debug for GroupConsumerCheckpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerCheckpoint")
            .field("topic", &self.topic())
            .field("partition", &self.partition())
            .field("next_offset", &self.next_offset())
            .finish_non_exhaustive()
    }
}
