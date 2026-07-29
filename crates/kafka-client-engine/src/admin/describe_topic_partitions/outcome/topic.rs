//! Stable engine-owned topic facts for one explicit page.

use super::AdminDescribeTopicPartition;

/// One requested topic and its broker-provided partition subset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTopicPartitionsTopic {
    pub(super) error_code: i16,
    pub(super) name: String,
    pub(super) topic_id: [u8; 16],
    pub(super) internal: bool,
    pub(super) partitions: Vec<AdminDescribeTopicPartition>,
    pub(super) authorized_operations: i32,
}

impl AdminDescribeTopicPartitionsTopic {
    /// Returns Kafka's exact signed topic error.
    pub const fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the exact requested topic identity.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns all sixteen topic-ID bytes, including all-zero values.
    pub const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Reports Kafka's internal-topic flag.
    pub const fn internal(&self) -> bool {
        self.internal
    }

    /// Returns partitions in broker page order.
    pub fn partitions(&self) -> &[AdminDescribeTopicPartition] {
        &self.partitions
    }

    /// Returns Kafka's exact authorized-operations bitfield or sentinel.
    pub const fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }

    /// Consumes every exact topic fact into stable owned parts.
    pub fn into_parts(
        self,
    ) -> (
        i16,
        String,
        [u8; 16],
        bool,
        Vec<AdminDescribeTopicPartition>,
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
