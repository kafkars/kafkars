//! Stable engine-owned page and explicit next-cursor facts.

use crate::admin::describe_topic_partitions::AdminDescribeTopicPartitionsCursor;

use super::AdminDescribeTopicPartitionsTopic;

/// One explicit page; a next cursor never triggers hidden work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTopicPartitionsPage {
    pub(super) throttle_time_ms: u32,
    pub(super) topics: Vec<AdminDescribeTopicPartitionsTopic>,
    pub(super) next_cursor: Option<AdminDescribeTopicPartitionsCursor>,
}

impl AdminDescribeTopicPartitionsPage {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns response topics restored to request order.
    pub fn topics(&self) -> &[AdminDescribeTopicPartitionsTopic] {
        &self.topics
    }

    /// Returns the cursor for an independently submitted next page.
    pub const fn next_cursor(&self) -> Option<&AdminDescribeTopicPartitionsCursor> {
        self.next_cursor.as_ref()
    }

    /// Consumes this page into stable owned parts.
    pub fn into_parts(
        self,
    ) -> (
        u32,
        Vec<AdminDescribeTopicPartitionsTopic>,
        Option<AdminDescribeTopicPartitionsCursor>,
    ) {
        (self.throttle_time_ms, self.topics, self.next_cursor)
    }
}
