//! Generated-free normalized API-key 75 page and cursor facts.

use super::NormalizedDescribeTopicPartitionsTopic;

/// Optional broker cursor for a separately submitted page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeTopicPartitionsCursor {
    topic_name: String,
    partition_index: i32,
}

impl NormalizedDescribeTopicPartitionsCursor {
    pub(super) const fn new(topic_name: String, partition_index: i32) -> Self {
        Self {
            topic_name,
            partition_index,
        }
    }

    pub(crate) fn into_parts(self) -> (String, i32) {
        (self.topic_name, self.partition_index)
    }

    pub(super) fn topic_name(&self) -> &String {
        &self.topic_name
    }

    #[cfg(test)]
    pub(crate) fn topic_name_str(&self) -> &str {
        &self.topic_name
    }

    #[cfg(test)]
    pub(crate) const fn partition_index(&self) -> i32 {
        self.partition_index
    }
}

/// One bounded v0 response page without generated protocol values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeTopicPartitionsResponse {
    throttle_time_ms: u32,
    topics: Vec<NormalizedDescribeTopicPartitionsTopic>,
    next_cursor: Option<NormalizedDescribeTopicPartitionsCursor>,
    retained_bytes: usize,
}

impl NormalizedDescribeTopicPartitionsResponse {
    pub(super) const fn new(
        throttle_time_ms: u32,
        topics: Vec<NormalizedDescribeTopicPartitionsTopic>,
        next_cursor: Option<NormalizedDescribeTopicPartitionsCursor>,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            topics,
            next_cursor,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        u32,
        Vec<NormalizedDescribeTopicPartitionsTopic>,
        Option<NormalizedDescribeTopicPartitionsCursor>,
        usize,
    ) {
        (
            self.throttle_time_ms,
            self.topics,
            self.next_cursor,
            self.retained_bytes,
        )
    }

    pub(super) fn topics(&self) -> &[NormalizedDescribeTopicPartitionsTopic] {
        &self.topics
    }

    pub(super) const fn next_cursor(&self) -> Option<&NormalizedDescribeTopicPartitionsCursor> {
        self.next_cursor.as_ref()
    }

    pub(super) fn topic_capacity(&self) -> usize {
        self.topics.capacity()
    }

    #[cfg(test)]
    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    #[cfg(test)]
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}
