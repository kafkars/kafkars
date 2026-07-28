//! Stable engine assignment values for KIP-848 group descriptions.

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

    /// Consumes this topic assignment into stable parts.
    pub fn into_parts(self) -> ([u8; 16], String, Vec<i32>) {
        (self.topic_id, self.topic_name, self.partitions)
    }
}

/// One current or target assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupAssignment {
    topics: Vec<ConsumerGroupTopicPartitions>,
}

impl ConsumerGroupAssignment {
    pub(crate) const fn new(topics: Vec<ConsumerGroupTopicPartitions>) -> Self {
        Self { topics }
    }

    /// Consumes the assignment into canonical topic assignments.
    pub fn into_topics(self) -> Vec<ConsumerGroupTopicPartitions> {
        self.topics
    }
}
