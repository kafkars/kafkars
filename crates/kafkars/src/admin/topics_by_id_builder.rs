//! Inert topic-ID `DescribeTopics` options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, admin_topics_request::DescribeTopicsAdminRequest};

use super::DescribeTopicsById;

/// Inert caller-ordered topic-ID description request.
#[must_use = "call submit to admit the DescribeTopicsById operation"]
pub struct DescribeTopicsByIdBuilder {
    engine: AdminEngine,
    request: DescribeTopicsAdminRequest,
    timeout: Duration,
}

impl DescribeTopicsByIdBuilder {
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

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> DescribeTopicsById {
        DescribeTopicsById::from_bridge(
            self.engine
                .submit_describe_topics_by_id(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeTopicsByIdBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeTopicsByIdBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
