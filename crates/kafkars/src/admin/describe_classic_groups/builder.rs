//! Inert caller-ordered classic-group description intent.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_describe_consumer_groups::DescribeConsumerGroupsAdminRequest,
};

use super::DescribeClassicGroups;

/// Inert classic-group description request.
#[must_use = "call submit to admit the DescribeClassicGroups operation"]
pub struct DescribeClassicGroupsBuilder {
    engine: AdminEngine,
    request: DescribeConsumerGroupsAdminRequest,
    timeout: Duration,
}

impl DescribeClassicGroupsBuilder {
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
    pub fn submit(self) -> DescribeClassicGroups {
        DescribeClassicGroups::from_bridge(
            self.engine
                .submit_describe_classic_groups(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeClassicGroupsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeClassicGroupsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
