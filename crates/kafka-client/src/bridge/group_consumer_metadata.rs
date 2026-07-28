//! Private ownership of exact engine classic-group transaction metadata.

use kafka_client_engine::GroupConsumerMetadata as EngineMetadata;

/// Cloneable opaque assignment fence retained beside stable facade fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupConsumerMetadata {
    inner: EngineMetadata,
}

impl GroupConsumerMetadata {
    pub(crate) const fn from_engine(inner: EngineMetadata) -> Self {
        Self { inner }
    }

    pub(crate) fn group(&self) -> &str {
        self.inner.group()
    }

    pub(crate) fn member(&self) -> &str {
        self.inner.member()
    }

    pub(crate) fn generation_id(&self) -> i32 {
        self.inner.generation_id()
    }

    pub(crate) fn assignment_epoch(&self) -> u64 {
        self.inner.assignment_epoch()
    }
}
