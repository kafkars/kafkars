//! Inert cluster-wide consumer-group listing intent.

use std::time::Duration;

use crate::bridge::admin::AdminEngine;

use super::ListConsumerGroups;

/// Inert cluster-wide consumer-group listing request.
#[must_use = "call submit to admit the ListConsumerGroups operation"]
pub struct ListConsumerGroupsBuilder {
    engine: AdminEngine,
    timeout: Duration,
}

impl ListConsumerGroupsBuilder {
    pub(crate) const fn new(engine: AdminEngine, timeout: Duration) -> Self {
        Self { engine, timeout }
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> ListConsumerGroups {
        ListConsumerGroups::from_bridge(self.engine.submit_list_consumer_groups(self.timeout))
    }
}

impl std::fmt::Debug for ListConsumerGroupsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListConsumerGroupsBuilder")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
