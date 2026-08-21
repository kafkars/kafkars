//! Inert intent for exactly one Admin `DescribeTopicPartitions` v0 page.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, describe_topic_partitions::DescribeTopicPartitionsAdminRequest,
};

use super::{DescribeTopicPartitions, DescribeTopicPartitionsCursor};

const DEFAULT_RESPONSE_PARTITION_LIMIT: u32 = 2_000;

/// Inert caller-ordered request for one topic-partition page.
#[must_use = "call submit to admit the DescribeTopicPartitions operation"]
pub struct DescribeTopicPartitionsBuilder {
    engine: AdminEngine,
    topics: Vec<String>,
    response_partition_limit: u32,
    cursor: Option<DescribeTopicPartitionsCursor>,
    timeout: Duration,
}

impl DescribeTopicPartitionsBuilder {
    pub(crate) const fn new(engine: AdminEngine, topics: Vec<String>, timeout: Duration) -> Self {
        Self {
            engine,
            topics,
            response_partition_limit: DEFAULT_RESPONSE_PARTITION_LIMIT,
            cursor: None,
            timeout,
        }
    }

    /// Sets the positive maximum partition count for this response page.
    ///
    /// Validation remains deferred until [`Self::submit`].
    pub const fn response_partition_limit(mut self, limit: u32) -> Self {
        self.response_partition_limit = limit;
        self
    }

    /// Starts this explicit page at the supplied topic and partition cursor.
    pub fn cursor(mut self, cursor: DescribeTopicPartitionsCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures one public deadline and attempts bounded one-page admission.
    ///
    /// A returned next cursor is data only. This operation never submits
    /// another page implicitly.
    pub fn submit(self) -> DescribeTopicPartitions {
        let cursor = self.cursor.map(DescribeTopicPartitionsCursor::into_parts);
        let request = DescribeTopicPartitionsAdminRequest::new(
            self.topics,
            self.response_partition_limit,
            cursor,
        );
        DescribeTopicPartitions::from_bridge(
            self.engine
                .submit_describe_topic_partitions(request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeTopicPartitionsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeTopicPartitionsBuilder")
            .field("topics", &self.topics)
            .field("response_partition_limit", &self.response_partition_limit)
            .field("cursor", &self.cursor)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
