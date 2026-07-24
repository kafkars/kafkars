//! Cloneable public admin handle over the private engine bridge.

use crate::bridge::admin::{AdminEngine, AdminRequest, DeleteAdminRequest};

use super::{CreateTopicsBuilder, DeleteTopicsBuilder, DescribeClusterBuilder, NewTopic};

/// Cheaply cloneable, thread-safe admin handle.
#[derive(Debug, Clone)]
pub struct Admin {
    engine: AdminEngine,
}

impl Admin {
    pub(crate) const fn new(engine: AdminEngine) -> Self {
        Self { engine }
    }

    /// Builds an inert ordered `CreateTopics` request.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`CreateTopicsBuilder::submit`] is called.
    pub fn create_topics<I>(&self, topics: I) -> CreateTopicsBuilder
    where
        I: IntoIterator<Item = NewTopic>,
    {
        let request = AdminRequest::from_topics(topics);
        CreateTopicsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert ordered name-based `DeleteTopics` request.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DeleteTopicsBuilder::submit`] is called.
    pub fn delete_topics<I, T>(&self, topics: I) -> DeleteTopicsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let request = DeleteAdminRequest::from_topics(topics);
        DeleteTopicsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert broker-endpoint `DescribeCluster` request.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeClusterBuilder::submit`] is called.
    pub fn describe_cluster(&self) -> DescribeClusterBuilder {
        DescribeClusterBuilder::new(self.engine.clone(), self.engine.default_timeout())
    }
}
