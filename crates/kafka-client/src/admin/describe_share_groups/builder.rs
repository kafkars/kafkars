//! Builder for one caller-ordered, multi-group ShareGroup description operation.

use std::{fmt, time::Duration};

use crate::bridge::{admin::AdminEngine, describe_share_groups::DescribeShareGroupsAdminRequest};

use super::DescribeShareGroups;

/// Configures and submits a caller-ordered batch of ShareGroup descriptions.
#[must_use = "the builder does nothing until submit is called"]
pub struct DescribeShareGroupsBuilder {
    engine: AdminEngine,
    request: DescribeShareGroupsAdminRequest,
    timeout: Duration,
}

impl DescribeShareGroupsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeShareGroupsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Requests the broker-authorized operations for every described group.
    pub fn include_authorized_operations(mut self, include: bool) -> Self {
        self.request.set_include_authorized_operations(include);
        self
    }

    /// Sets the operation deadline relative to submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the deadline and submits the operation to the client engine.
    pub fn submit(self) -> DescribeShareGroups {
        DescribeShareGroups::from_bridge(
            self.engine
                .submit_describe_share_groups(self.request, self.timeout),
        )
    }
}

impl fmt::Debug for DescribeShareGroupsBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeShareGroupsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
