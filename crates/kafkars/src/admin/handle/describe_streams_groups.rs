//! Batched `StreamsGroup` description entry point on the shared admin handle.

use super::Admin;
use crate::{
    admin::DescribeStreamsGroupsBuilder,
    bridge::describe_streams_groups::DescribeStreamsGroupsAdminRequest,
};

impl Admin {
    /// Builds inert caller-ordered descriptions for multiple `StreamsGroups`.
    ///
    /// Group validation remains deferred until
    /// [`DescribeStreamsGroupsBuilder::submit`] captures the public absolute
    /// deadline and attempts one bounded batch admission.
    pub fn describe_streams_groups<I, T>(&self, group_ids: I) -> DescribeStreamsGroupsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        DescribeStreamsGroupsBuilder::new(
            self.engine.clone(),
            DescribeStreamsGroupsAdminRequest::new(group_ids.into_iter().map(Into::into).collect()),
            self.engine.default_timeout(),
        )
    }
}
