//! Facade-owned direct-assignment and initial-position values.

/// Initial position for one directly assigned topic-partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartPosition {
    /// Resolve Kafka's earliest available offset.
    Beginning,
    /// Resolve Kafka's end offset.
    End,
    /// Begin at this exact nonnegative next-fetch offset.
    Offset(i64),
}

/// One facade-owned Kafka topic-partition prepared for direct assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicPartition {
    topic: String,
    partition: i32,
    start: Option<StartPosition>,
}

impl TopicPartition {
    /// Creates one topic-partition without inventing an initial-position policy.
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            start: None,
        }
    }

    /// Sets the explicit initial position used by direct assignment.
    #[must_use]
    pub const fn start_at(mut self, start: StartPosition) -> Self {
        self.start = Some(start);
        self
    }

    /// Returns the Kafka topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the signed Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the explicit initial position, if supplied.
    pub const fn start_position(&self) -> Option<StartPosition> {
        self.start
    }

    pub(crate) fn into_parts(self) -> (String, i32, Option<StartPosition>) {
        (self.topic, self.partition, self.start)
    }
}
