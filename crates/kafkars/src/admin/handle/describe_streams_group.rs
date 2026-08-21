//! Singular `StreamsGroup` description entry point on the shared admin handle.

use super::Admin;
use crate::{
    admin::DescribeStreamsGroupBuilder,
    bridge::describe_streams_group::DescribeStreamsGroupAdminRequest,
};

impl Admin {
    /// Builds an inert typed description request for one modern `StreamsGroup`.
    ///
    /// Group validation remains deferred until
    /// [`DescribeStreamsGroupBuilder::submit`] captures the public absolute
    /// deadline and attempts bounded engine admission.
    pub fn describe_streams_group(
        &self,
        group_id: impl Into<String>,
    ) -> DescribeStreamsGroupBuilder {
        DescribeStreamsGroupBuilder::new(
            self.engine.clone(),
            DescribeStreamsGroupAdminRequest::new(group_id.into()),
            self.engine.default_timeout(),
        )
    }
}
