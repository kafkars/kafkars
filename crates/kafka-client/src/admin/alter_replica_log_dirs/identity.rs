//! Stable topic-partition replica identity shared by broker-scoped admin APIs.

/// One topic-partition replica hosted by an explicit broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicPartitionReplica {
    topic: String,
    partition: i32,
    broker_id: i32,
}

impl TopicPartitionReplica {
    /// Creates inert replica identity validated when an operation is submitted.
    pub fn new(topic: impl Into<String>, partition: i32, broker_id: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            broker_id,
        }
    }

    /// Returns the Kafka topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the signed partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the signed broker identity.
    pub const fn broker_id(&self) -> i32 {
        self.broker_id
    }

    pub(crate) fn into_parts(self) -> (String, i32, i32) {
        (self.topic, self.partition, self.broker_id)
    }
}
