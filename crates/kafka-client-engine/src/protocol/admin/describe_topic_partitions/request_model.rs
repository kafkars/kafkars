//! Generated-free borrowed intent for one API-key 75 page request.

/// Borrowed first topic-partition cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DescribeTopicPartitionsRequestCursor<'a> {
    topic_name: &'a str,
    partition_index: i32,
}

impl<'a> DescribeTopicPartitionsRequestCursor<'a> {
    pub(crate) const fn new(topic_name: &'a str, partition_index: i32) -> Self {
        Self {
            topic_name,
            partition_index,
        }
    }

    pub(super) const fn topic_name(self) -> &'a str {
        self.topic_name
    }

    pub(super) const fn partition_index(self) -> i32 {
        self.partition_index
    }
}

/// Borrowed caller-order topic selection and explicit page controls.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DescribeTopicPartitionsRequestPlan<'a> {
    topics: &'a [String],
    response_partition_limit: u32,
    cursor: Option<DescribeTopicPartitionsRequestCursor<'a>>,
}

impl<'a> DescribeTopicPartitionsRequestPlan<'a> {
    pub(crate) const fn new(
        topics: &'a [String],
        response_partition_limit: u32,
        cursor: Option<DescribeTopicPartitionsRequestCursor<'a>>,
    ) -> Self {
        Self {
            topics,
            response_partition_limit,
            cursor,
        }
    }

    pub(super) const fn topics(self) -> &'a [String] {
        self.topics
    }

    pub(super) const fn response_partition_limit(self) -> u32 {
        self.response_partition_limit
    }

    pub(super) const fn cursor(self) -> Option<DescribeTopicPartitionsRequestCursor<'a>> {
        self.cursor
    }
}
