//! Generated-free exact topic facts for one API-key 75 page.

use super::NormalizedDescribeTopicPartition;

/// One topic preserving nullable identity and exact broker-owned scalars.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeTopicPartitionsTopic {
    error_code: i16,
    name: Option<String>,
    topic_id: [u8; 16],
    internal: bool,
    partitions: Vec<NormalizedDescribeTopicPartition>,
    authorized_operations: i32,
}

impl NormalizedDescribeTopicPartitionsTopic {
    pub(super) const fn new(
        error_code: i16,
        name: Option<String>,
        topic_id: [u8; 16],
        internal: bool,
        partitions: Vec<NormalizedDescribeTopicPartition>,
        authorized_operations: i32,
    ) -> Self {
        Self {
            error_code,
            name,
            topic_id,
            internal,
            partitions,
            authorized_operations,
        }
    }

    /// Consumes every exact topic fact into host-owned parts.
    pub(crate) fn into_parts(
        self,
    ) -> (
        i16,
        Option<String>,
        [u8; 16],
        bool,
        Vec<NormalizedDescribeTopicPartition>,
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

    pub(super) fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }

    pub(super) fn partitions(&self) -> &[NormalizedDescribeTopicPartition] {
        &self.partitions
    }

    pub(super) fn partition_capacity(&self) -> usize {
        self.partitions.capacity()
    }

    #[cfg(test)]
    pub(crate) const fn scalar_parts(&self) -> (i16, [u8; 16], bool, i32) {
        (
            self.error_code,
            self.topic_id,
            self.internal,
            self.authorized_operations,
        )
    }

    #[cfg(test)]
    pub(crate) fn topic_name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}
