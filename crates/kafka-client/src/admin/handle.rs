//! Cloneable public admin handle over the private engine bridge.

use crate::bridge::admin::{AdminEngine, AdminRequest};

use super::{CreateTopicsBuilder, NewTopic};

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
}
