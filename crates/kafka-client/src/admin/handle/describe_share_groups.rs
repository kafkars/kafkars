//! Batched ShareGroup description entry point on the shared admin handle.

use super::Admin;
use crate::{
    admin::DescribeShareGroupsBuilder,
    bridge::describe_share_groups::DescribeShareGroupsAdminRequest,
};

impl Admin {
    /// Builds inert caller-ordered descriptions for multiple modern ShareGroups.
    ///
    /// Group validation remains deferred until
    /// [`DescribeShareGroupsBuilder::submit`] captures the public absolute
    /// deadline and attempts one bounded batch admission.
    pub fn describe_share_groups<I, T>(&self, group_ids: I) -> DescribeShareGroupsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        DescribeShareGroupsBuilder::new(
            self.engine.clone(),
            DescribeShareGroupsAdminRequest::new(group_ids.into_iter().map(Into::into).collect()),
            self.engine.default_timeout(),
        )
    }
}
