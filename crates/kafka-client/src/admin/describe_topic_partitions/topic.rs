//! Stable API-key 75 topic page entry with topic-scoped broker outcome.

use crate::KafkaError;

use super::DescribeTopicPartition;

/// One requested topic and the partition subset carried by this page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicPartitionsTopic {
    error: Option<KafkaError>,
    name: String,
    topic_id: [u8; 16],
    internal: bool,
    partitions: Vec<DescribeTopicPartition>,
    authorized_operations: i32,
}

impl DescribeTopicPartitionsTopic {
    pub(crate) const fn new(
        error: Option<KafkaError>,
        name: String,
        topic_id: [u8; 16],
        internal: bool,
        partitions: Vec<DescribeTopicPartition>,
        authorized_operations: i32,
    ) -> Self {
        Self {
            error,
            name,
            topic_id,
            internal,
            partitions,
            authorized_operations,
        }
    }

    /// Returns Kafka's topic-scoped error with its exact signed code.
    pub const fn error(&self) -> Option<&KafkaError> {
        self.error.as_ref()
    }

    /// Returns the exact requested topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the exact sixteen topic-ID bytes, including an all-zero value.
    pub const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Returns whether Kafka marks this topic as internal.
    pub const fn is_internal(&self) -> bool {
        self.internal
    }

    /// Returns partitions in Kafka's order within this explicit page.
    pub fn partitions(&self) -> &[DescribeTopicPartition] {
        &self.partitions
    }

    /// Returns Kafka's exact authorized-operations bitfield or sentinel.
    pub const fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }
}
