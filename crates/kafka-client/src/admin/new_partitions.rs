//! Stable Rust construction of one automatic partition-count increase.

/// One topic and its requested new total partition count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPartitions {
    topic: String,
    total_count: i32,
}

impl NewPartitions {
    /// Creates a broker-assigned partition increase.
    pub fn new(topic: impl Into<String>, total_count: i32) -> Self {
        Self {
            topic: topic.into(),
            total_count,
        }
    }

    /// Returns the requested topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the requested new total partition count.
    pub const fn total_count(&self) -> i32 {
        self.total_count
    }

    pub(crate) fn into_parts(self) -> (String, i32) {
        (self.topic, self.total_count)
    }
}
