//! Stable explicit cursor for one separately submitted API-key 75 page.

/// First topic and partition eligible for one page request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicPartitionsCursor {
    topic_name: String,
    partition_index: i32,
}

impl DescribeTopicPartitionsCursor {
    /// Creates inert cursor intent validated only when its builder is submitted.
    pub fn new(topic_name: impl Into<String>, partition_index: i32) -> Self {
        Self {
            topic_name: topic_name.into(),
            partition_index,
        }
    }

    /// Returns the exact requested topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns the requested first partition index.
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    pub(crate) fn into_parts(self) -> (String, i32) {
        (self.topic_name, self.partition_index)
    }
}
