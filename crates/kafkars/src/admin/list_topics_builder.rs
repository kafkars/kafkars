//! Inert all-topic options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, admin_topics_request::DescribeTopicsAdminRequest};

use super::ListTopics;

/// Inert query for topic descriptions visible to the authenticated principal.
#[must_use = "call submit to admit the ListTopics operation"]
pub struct ListTopicsBuilder {
    engine: AdminEngine,
    request: DescribeTopicsAdminRequest,
    timeout: Duration,
}

impl ListTopicsBuilder {
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

    /// Selects whether broker-marked internal topics enter the result.
    pub fn include_internal(mut self, include_internal: bool) -> Self {
        self.request = self.request.with_include_internal(include_internal);
        self
    }

    /// Selects whether Kafka should return exact topic authorization bitfields.
    pub fn include_authorized_operations(mut self, include: bool) -> Self {
        self.request = self.request.with_authorized_operations(include);
        self
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    pub fn submit(self) -> ListTopics {
        ListTopics::from_bridge(
            self.engine
                .submit_describe_topics(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for ListTopicsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListTopicsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
