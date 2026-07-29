//! Stable result for exactly one explicit API-key 75 response page.

use std::time::Duration;

use super::{DescribeTopicPartitionsCursor, DescribeTopicPartitionsTopic};

/// One completed page; its next cursor never starts hidden work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicPartitionsPage {
    throttle_time: Duration,
    topics: Vec<DescribeTopicPartitionsTopic>,
    next_cursor: Option<DescribeTopicPartitionsCursor>,
}

impl DescribeTopicPartitionsPage {
    pub(crate) const fn new(
        throttle_time: Duration,
        topics: Vec<DescribeTopicPartitionsTopic>,
        next_cursor: Option<DescribeTopicPartitionsCursor>,
    ) -> Self {
        Self {
            throttle_time,
            topics,
            next_cursor,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns only the topics carried by this explicit page.
    ///
    /// Entries remain in caller topic order. Each entry retains its own
    /// topic-scoped broker error, if any.
    pub fn topics(&self) -> &[DescribeTopicPartitionsTopic] {
        &self.topics
    }

    /// Returns the cursor a caller may place on a separately submitted page.
    pub const fn next_cursor(&self) -> Option<&DescribeTopicPartitionsCursor> {
        self.next_cursor.as_ref()
    }

    /// Consumes this one-page result into stable scalar parts.
    pub fn into_parts(
        self,
    ) -> (
        Duration,
        Vec<DescribeTopicPartitionsTopic>,
        Option<DescribeTopicPartitionsCursor>,
    ) {
        (self.throttle_time, self.topics, self.next_cursor)
    }
}
