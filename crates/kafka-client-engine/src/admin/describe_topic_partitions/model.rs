//! Engine-owned inert intent for one explicit topic-partition page.

use kafka_client_core::{
    DescribeTopicPartitionsCursor, DescribeTopicPartitionsPlan, DescribeTopicPartitionsPlanError,
};

/// Stable explicit cursor for a first or separately requested next page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTopicPartitionsCursor {
    topic_name: String,
    partition_index: i32,
}

impl AdminDescribeTopicPartitionsCursor {
    /// Creates inert cursor intent validated at the public operation boundary.
    pub const fn new(topic_name: String, partition_index: i32) -> Self {
        Self {
            topic_name,
            partition_index,
        }
    }

    /// Returns the exact topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns the requested first partition index.
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Consumes this cursor into stable scalar parts.
    pub fn into_parts(self) -> (String, i32) {
        (self.topic_name, self.partition_index)
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.topic_name = self.topic_name.into_boxed_str().into_string();
        self
    }

    pub(crate) fn into_core(
        self,
    ) -> Result<DescribeTopicPartitionsCursor, DescribeTopicPartitionsPlanError> {
        DescribeTopicPartitionsCursor::new(self.topic_name, self.partition_index)
    }
}

/// Caller-ordered selection and explicit controls for exactly one page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTopicPartitionsRequest {
    topics: Vec<String>,
    response_partition_limit: u32,
    cursor: Option<AdminDescribeTopicPartitionsCursor>,
}

impl AdminDescribeTopicPartitionsRequest {
    /// Creates inert page intent validated only after deadline capture.
    pub const fn new(
        topics: Vec<String>,
        response_partition_limit: u32,
        cursor: Option<AdminDescribeTopicPartitionsCursor>,
    ) -> Self {
        Self {
            topics,
            response_partition_limit,
            cursor,
        }
    }

    /// Consumes this inert request into stable owned parts.
    pub fn into_parts(self) -> (Vec<String>, u32, Option<AdminDescribeTopicPartitionsCursor>) {
        (self.topics, self.response_partition_limit, self.cursor)
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.topics = self
            .topics
            .into_iter()
            .map(|topic| topic.into_boxed_str().into_string())
            .collect();
        self.topics.shrink_to_fit();
        self.cursor = self
            .cursor
            .map(AdminDescribeTopicPartitionsCursor::canonicalize);
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<DescribeTopicPartitionsPlan, DescribeTopicPartitionsPlanError> {
        let cursor = self
            .cursor
            .map(AdminDescribeTopicPartitionsCursor::into_core)
            .transpose()?;
        DescribeTopicPartitionsPlan::new(self.topics, self.response_partition_limit, cursor)
    }
}
