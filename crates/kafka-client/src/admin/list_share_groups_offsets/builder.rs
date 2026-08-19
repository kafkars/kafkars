//! Inert multi-ShareGroup offset options with one submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, list_share_group_offsets::ListShareGroupsOffsetsAdminRequest,
};

use super::ListShareGroupsOffsets;

/// Inert caller-ordered offset queries for multiple `ShareGroups`.
#[must_use = "call submit to admit the ListShareGroupsOffsets operation"]
pub struct ListShareGroupsOffsetsBuilder {
    engine: AdminEngine,
    request: ListShareGroupsOffsetsAdminRequest,
    timeout: Duration,
}

impl ListShareGroupsOffsetsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: ListShareGroupsOffsetsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> ListShareGroupsOffsets {
        ListShareGroupsOffsets::from_bridge(
            self.engine
                .submit_list_share_groups_offsets(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for ListShareGroupsOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListShareGroupsOffsetsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
