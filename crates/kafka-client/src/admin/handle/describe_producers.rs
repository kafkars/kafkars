//! Producer-description entry point on the shared public admin handle.

use super::Admin;
use crate::{
    TopicPartition, admin::DescribeProducersBuilder,
    bridge::describe_producers::DescribeProducersAdminRequest,
};

impl Admin {
    /// Builds an inert caller-ordered active-producer query.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeProducersBuilder::submit`] is called.
    pub fn describe_producers<I>(&self, targets: I) -> DescribeProducersBuilder
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        DescribeProducersBuilder::new(
            self.engine.clone(),
            DescribeProducersAdminRequest::new(targets.into_iter().collect()),
            self.engine.default_timeout(),
        )
    }
}
