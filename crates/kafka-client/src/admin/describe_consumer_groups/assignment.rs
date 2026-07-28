//! Public typed KIP-848 current and target assignments.

/// One topic and its assigned partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupTopicPartitions {
    topic_id: [u8; 16],
    topic_name: String,
    partitions: Vec<i32>,
}

impl ConsumerGroupTopicPartitions {
    pub(crate) const fn new(topic_id: [u8; 16], topic_name: String, partitions: Vec<i32>) -> Self {
        Self {
            topic_id,
            topic_name,
            partitions,
        }
    }

    /// Returns Kafka's topic UUID bytes.
    pub const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Returns Kafka's topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns canonical nonnegative partition indexes.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// One current or target assignment canonicalized by topic identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupAssignment {
    topics: Vec<ConsumerGroupTopicPartitions>,
}

impl ConsumerGroupAssignment {
    pub(crate) const fn new(topics: Vec<ConsumerGroupTopicPartitions>) -> Self {
        Self { topics }
    }

    /// Returns canonical topic assignments.
    pub fn topics(&self) -> &[ConsumerGroupTopicPartitions] {
        &self.topics
    }
}
