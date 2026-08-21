//! Inert one-StreamsGroup description intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, describe_streams_group::DescribeStreamsGroupAdminRequest};

use super::DescribeStreamsGroup;

/// Inert typed description request for one modern `StreamsGroup`.
#[must_use = "call submit to admit the DescribeStreamsGroup operation"]
pub struct DescribeStreamsGroupBuilder {
    engine: AdminEngine,
    request: DescribeStreamsGroupAdminRequest,
    timeout: Duration,
}

impl DescribeStreamsGroupBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeStreamsGroupAdminRequest,
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

    /// Requests Kafka's optional full topology description.
    ///
    /// Enabling this requires `StreamsGroupDescribe` v1.
    pub fn include_topology_description(mut self, include: bool) -> Self {
        self.request.set_include_topology_description(include);
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> DescribeStreamsGroup {
        DescribeStreamsGroup::from_bridge(
            self.engine
                .submit_describe_streams_group(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeStreamsGroupBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeStreamsGroupBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
