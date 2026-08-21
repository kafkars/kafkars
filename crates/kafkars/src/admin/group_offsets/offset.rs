//! Stable committed consumer-group offset facts owned by the Rust facade.

/// One broker-visible committed position and its optional Kafka metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupOffset {
    committed_offset: Option<i64>,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

impl ConsumerGroupOffset {
    pub(crate) const fn new(
        committed_offset: Option<i64>,
        leader_epoch: Option<i32>,
        metadata: Option<String>,
    ) -> Self {
        Self {
            committed_offset,
            leader_epoch,
            metadata,
        }
    }

    /// Returns the committed next offset, or `None` when Kafka reports no offset.
    pub const fn committed_offset(&self) -> Option<i64> {
        self.committed_offset
    }

    /// Returns the committed leader epoch when Kafka supplied one.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Returns Kafka's nullable committed metadata.
    pub fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    /// Consumes this offset into stable scalar parts.
    pub fn into_parts(self) -> (Option<i64>, Option<i32>, Option<String>) {
        (self.committed_offset, self.leader_epoch, self.metadata)
    }
}
