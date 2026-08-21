//! Inert one-ShareGroup description intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, describe_share_group::DescribeShareGroupAdminRequest};

use super::DescribeShareGroup;

/// Inert typed description request for one modern `ShareGroup`.
#[must_use = "call submit to admit the DescribeShareGroup operation"]
pub struct DescribeShareGroupBuilder {
    engine: AdminEngine,
    request: DescribeShareGroupAdminRequest,
    timeout: Duration,
}

impl DescribeShareGroupBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeShareGroupAdminRequest,
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
    pub fn submit(self) -> DescribeShareGroup {
        DescribeShareGroup::from_bridge(
            self.engine
                .submit_describe_share_group(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeShareGroupBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeShareGroupBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
