//! Stable typed `ShareGroup` topic-partition assignment values.

/// One topic and its assigned partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareGroupTopicPartitions {
    topic_id: [u8; 16],
    topic_name: String,
    partitions: Vec<i32>,
}

impl ShareGroupTopicPartitions {
    pub(crate) const fn new(topic_id: [u8; 16], topic_name: String, partitions: Vec<i32>) -> Self {
        Self {
            topic_id,
            topic_name,
            partitions,
        }
    }

    /// Returns Kafka's nonzero topic identity.
    pub const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Returns the correlated UTF-8 topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns nonnegative partitions in deterministic ascending order.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// One member's typed `ShareGroup` assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareGroupAssignment {
    topics: Vec<ShareGroupTopicPartitions>,
}

impl ShareGroupAssignment {
    pub(crate) const fn new(topics: Vec<ShareGroupTopicPartitions>) -> Self {
        Self { topics }
    }

    /// Returns assigned topics in topic-ID order, then topic-name byte order.
    pub fn topics(&self) -> &[ShareGroupTopicPartitions] {
        &self.topics
    }

    /// Consumes the assignment into its deterministic topic list.
    pub fn into_topics(self) -> Vec<ShareGroupTopicPartitions> {
        self.topics
    }
}
