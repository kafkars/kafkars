//! Topic-partition page entry point on the shared public admin handle.

use super::Admin;
use crate::admin::DescribeTopicPartitionsBuilder;

impl Admin {
    /// Builds inert caller-ordered intent for one topic-partition page.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeTopicPartitionsBuilder::submit`] is called. A returned next
    /// cursor never triggers hidden pagination.
    pub fn describe_topic_partitions<I, S>(&self, topics: I) -> DescribeTopicPartitionsBuilder
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        DescribeTopicPartitionsBuilder::new(
            self.engine.clone(),
            topics.into_iter().map(Into::into).collect(),
            self.engine.default_timeout(),
        )
    }
}
