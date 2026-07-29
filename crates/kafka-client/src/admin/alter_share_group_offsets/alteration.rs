//! Stable inert intent for one ShareGroup partition offset alteration.

/// One caller-ordered ShareGroup partition offset alteration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareGroupOffsetAlteration {
    topic: String,
    partition: i32,
    start_offset: i64,
}

impl ShareGroupOffsetAlteration {
    /// Creates one alteration carrying Kafka's new ShareGroup start offset.
    pub fn new(topic: impl Into<String>, partition: i32, start_offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            start_offset,
        }
    }

    /// Returns the requested topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the requested partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the requested ShareGroup start offset.
    pub const fn start_offset(&self) -> i64 {
        self.start_offset
    }

    pub(crate) fn into_parts(self) -> (String, i32, i64) {
        (self.topic, self.partition, self.start_offset)
    }
}
