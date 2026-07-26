//! Inert `DescribeTopics` options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, admin_topics_request::DescribeTopicsAdminRequest};

use super::DescribeTopics;

/// Inert batched topic-description request.
#[must_use = "call submit to admit the DescribeTopics operation"]
pub struct DescribeTopicsBuilder {
    engine: AdminEngine,
    request: DescribeTopicsAdminRequest,
    timeout: Duration,
}

impl DescribeTopicsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeTopicsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    pub fn submit(self) -> DescribeTopics {
        DescribeTopics::from_bridge(
            self.engine
                .submit_describe_topics(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeTopicsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeTopicsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
