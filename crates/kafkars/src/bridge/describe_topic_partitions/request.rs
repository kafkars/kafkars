//! Inert public page intent translated only at the API-key 75 engine boundary.

use super::engine::{Cursor as EngineCursor, Request as EngineRequest};

/// Linear caller intent retained by the public builder before submission.
pub(crate) struct DescribeTopicPartitionsAdminRequest {
    topics: Vec<String>,
    response_partition_limit: u32,
    cursor: Option<(String, i32)>,
}

impl DescribeTopicPartitionsAdminRequest {
    pub(crate) const fn new(
        topics: Vec<String>,
        response_partition_limit: u32,
        cursor: Option<(String, i32)>,
    ) -> Self {
        Self {
            topics,
            response_partition_limit,
            cursor,
        }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.topics,
            self.response_partition_limit,
            self.cursor
                .map(|(topic, partition)| EngineCursor::new(topic, partition)),
        )
    }
}

impl std::fmt::Debug for DescribeTopicPartitionsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeTopicPartitionsAdminRequest")
            .field("topics", &self.topics)
            .field("response_partition_limit", &self.response_partition_limit)
            .field("cursor", &self.cursor)
            .finish()
    }
}
