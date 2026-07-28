//! Inert caller-ordered consumer-group description intent.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_describe_consumer_groups::DescribeConsumerGroupsAdminRequest,
};

use super::DescribeConsumerGroups;

/// Inert consumer-group description request.
#[must_use = "call submit to admit the DescribeConsumerGroups operation"]
pub struct DescribeConsumerGroupsBuilder {
    engine: AdminEngine,
    request: DescribeConsumerGroupsAdminRequest,
    timeout: Duration,
}

impl DescribeConsumerGroupsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeConsumerGroupsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Requests Kafka's raw authorized-operations bitfield.
    pub fn include_authorized_operations(mut self, include: bool) -> Self {
        self.request.set_include_authorized_operations(include);
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> DescribeConsumerGroups {
        DescribeConsumerGroups::from_bridge(
            self.engine
                .submit_describe_consumer_groups(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeConsumerGroupsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeConsumerGroupsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
