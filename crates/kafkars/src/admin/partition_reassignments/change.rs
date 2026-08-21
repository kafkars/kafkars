//! Stable inert intent for one partition-reassignment change.

/// One caller-ordered replica replacement or explicit cancellation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionReassignmentChange {
    topic: String,
    partition: i32,
    replicas: Option<Vec<i32>>,
}

impl PartitionReassignmentChange {
    /// Replaces the assignment with this exact ordered replica list.
    pub fn new<I>(topic: impl Into<String>, partition: i32, replicas: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        Self {
            topic: topic.into(),
            partition,
            replicas: Some(replicas.into_iter().collect()),
        }
    }

    /// Explicitly cancels an in-progress reassignment.
    pub fn cancel(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            replicas: None,
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

    /// Returns replacement replicas, or `None` for explicit cancellation.
    pub fn replicas(&self) -> Option<&[i32]> {
        self.replicas.as_deref()
    }

    pub(crate) fn into_parts(self) -> (String, i32, Option<Vec<i32>>) {
        (self.topic, self.partition, self.replicas)
    }
}
