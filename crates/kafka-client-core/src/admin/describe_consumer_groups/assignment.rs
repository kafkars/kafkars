//! Wire-free KIP-848 current and target assignment facts.

/// One topic and its assigned partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminConsumerGroupTopicPartitions {
    topic_id: [u8; 16],
    topic_name: String,
    partitions: Vec<i32>,
}

impl AdminConsumerGroupTopicPartitions {
    /// Creates one canonical topic assignment.
    pub const fn new(topic_id: [u8; 16], topic_name: String, partitions: Vec<i32>) -> Self {
        Self {
            topic_id,
            topic_name,
            partitions,
        }
    }

    /// Consumes this assignment into adapter-owned parts.
    pub fn into_parts(self) -> ([u8; 16], String, Vec<i32>) {
        (self.topic_id, self.topic_name, self.partitions)
    }
}

/// One current or target assignment canonicalized by topic identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminConsumerGroupAssignment {
    topics: Vec<AdminConsumerGroupTopicPartitions>,
}

impl AdminConsumerGroupAssignment {
    /// Creates one canonical assignment.
    pub const fn new(topics: Vec<AdminConsumerGroupTopicPartitions>) -> Self {
        Self { topics }
    }

    /// Consumes this assignment into its canonical topics.
    pub fn into_topics(self) -> Vec<AdminConsumerGroupTopicPartitions> {
        self.topics
    }
}
