//! Singular `ShareGroup` description entry point on the shared admin handle.

use super::Admin;
use crate::{
    admin::DescribeShareGroupBuilder, bridge::describe_share_group::DescribeShareGroupAdminRequest,
};

impl Admin {
    /// Builds an inert typed description request for one modern `ShareGroup`.
    ///
    /// Group validation remains deferred until [`DescribeShareGroupBuilder::submit`]
    /// captures the public absolute deadline and attempts bounded engine admission.
    pub fn describe_share_group(&self, group_id: impl Into<String>) -> DescribeShareGroupBuilder {
        DescribeShareGroupBuilder::new(
            self.engine.clone(),
            DescribeShareGroupAdminRequest::new(group_id.into()),
            self.engine.default_timeout(),
        )
    }
}
