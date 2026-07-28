//! Wire-free topic-partition assignments returned by `ConsumerGroupDescribe`.

/// One topic and its assigned partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerGroupDescribeTopicPartitions {
    topic_id: [u8; 16],
    topic_name: String,
    partitions: Vec<i32>,
}

impl ConsumerGroupDescribeTopicPartitions {
    pub(crate) const fn new(topic_id: [u8; 16], topic_name: String, partitions: Vec<i32>) -> Self {
        Self {
            topic_id,
            topic_name,
            partitions,
        }
    }

    pub(crate) const fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    pub(crate) fn topic_name(&self) -> &str {
        &self.topic_name
    }

    pub(crate) fn into_parts(self) -> ([u8; 16], String, Vec<i32>) {
        (self.topic_id, self.topic_name, self.partitions)
    }
}

/// One current or target assignment, canonicalized by topic identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerGroupDescribeAssignment {
    topics: Vec<ConsumerGroupDescribeTopicPartitions>,
}

impl ConsumerGroupDescribeAssignment {
    pub(crate) const fn new(topics: Vec<ConsumerGroupDescribeTopicPartitions>) -> Self {
        Self { topics }
    }

    #[cfg(test)]
    pub(crate) fn topics(&self) -> &[ConsumerGroupDescribeTopicPartitions] {
        &self.topics
    }

    pub(crate) fn into_topics(self) -> Vec<ConsumerGroupDescribeTopicPartitions> {
        self.topics
    }
}
