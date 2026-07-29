//! Builder for one caller-ordered, multi-group StreamsGroup description operation.

use std::{fmt, time::Duration};

use crate::bridge::{
    admin::AdminEngine, describe_streams_groups::DescribeStreamsGroupsAdminRequest,
};

use super::DescribeStreamsGroups;

/// Configures and submits a caller-ordered batch of StreamsGroup descriptions.
#[must_use = "the builder does nothing until submit is called"]
pub struct DescribeStreamsGroupsBuilder {
    engine: AdminEngine,
    request: DescribeStreamsGroupsAdminRequest,
    timeout: Duration,
}

impl DescribeStreamsGroupsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeStreamsGroupsAdminRequest,
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

    /// Requests Kafka's optional full topology description for every group.
    ///
    /// Enabling this requires StreamsGroupDescribe v1.
    pub fn include_topology_description(mut self, include: bool) -> Self {
        self.request.set_include_topology_description(include);
        self
    }

    /// Sets the operation deadline relative to submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the deadline and submits the operation to the client engine.
    pub fn submit(self) -> DescribeStreamsGroups {
        DescribeStreamsGroups::from_bridge(
            self.engine
                .submit_describe_streams_groups(self.request, self.timeout),
        )
    }
}

impl fmt::Debug for DescribeStreamsGroupsBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeStreamsGroupsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
