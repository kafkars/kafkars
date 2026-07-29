//! One exact protocol-normalized API-key 75 topic page entry.

use std::collections::BTreeSet;

use super::{DescribeTopicPartition, DescribeTopicPartitionsValueError};

const MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;

/// One requested topic and its ordered partition subset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicPartitionsTopic {
    error_code: i16,
    name: String,
    topic_id: [u8; 16],
    internal: bool,
    partitions: Vec<DescribeTopicPartition>,
    authorized_operations: i32,
}

impl DescribeTopicPartitionsTopic {
    /// Validates topic identity and duplicate-free nonnegative partition order.
    pub fn new(
        error_code: i16,
        name: String,
        topic_id: [u8; 16],
        internal: bool,
        partitions: Vec<DescribeTopicPartition>,
        authorized_operations: i32,
    ) -> Result<Self, DescribeTopicPartitionsValueError> {
        if name.is_empty() {
            return Err(DescribeTopicPartitionsValueError::EmptyTopicName);
        }
        if name.len() > MAX_TOPIC_NAME_BYTES {
            return Err(DescribeTopicPartitionsValueError::TopicNameTooLong);
        }
        let mut identities = BTreeSet::new();
        for partition in &partitions {
            if !identities.insert(partition.partition_index()) {
                return Err(DescribeTopicPartitionsValueError::DuplicatePartition);
            }
        }
        Ok(Self {
            error_code,
            name,
            topic_id,
            internal,
            partitions,
            authorized_operations,
        })
    }

    /// Returns Kafka's exact signed topic error code.
    pub const fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the exact requested topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the exact sixteen topic-ID bytes, including all-zero values.
    pub const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Reports Kafka's internal-topic flag.
    pub const fn internal(&self) -> bool {
        self.internal
    }

    /// Returns partitions in the broker's page order.
    pub fn partitions(&self) -> &[DescribeTopicPartition] {
        &self.partitions
    }

    /// Returns Kafka's exact authorized-operations bitfield or sentinel.
    pub const fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }

    /// Consumes the exact topic page entry into adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        i16,
        String,
        [u8; 16],
        bool,
        Vec<DescribeTopicPartition>,
        i32,
    ) {
        (
            self.error_code,
            self.name,
            self.topic_id,
            self.internal,
            self.partitions,
            self.authorized_operations,
        )
    }
}
