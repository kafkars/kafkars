//! `ShareGroup` offset-listing entry point on the shared admin handle.

use super::Admin;
use crate::{
    admin::{
        ListShareGroupOffsetsBuilder, ListShareGroupOffsetsQuery, ListShareGroupsOffsetsBuilder,
    },
    bridge::list_share_group_offsets::{
        ListShareGroupOffsetsAdminRequest, ListShareGroupsOffsetsAdminRequest,
    },
};

impl Admin {
    /// Builds an inert all-partition offset listing for one `ShareGroup`.
    ///
    /// [`ListShareGroupOffsetsBuilder::partitions`] narrows the query to an
    /// explicit nonempty caller-ordered selection. Validation remains deferred
    /// until [`ListShareGroupOffsetsBuilder::submit`] captures the public
    /// absolute deadline and attempts bounded engine admission.
    pub fn list_share_group_offsets(
        &self,
        group_id: impl Into<String>,
    ) -> ListShareGroupOffsetsBuilder {
        let request = ListShareGroupOffsetsAdminRequest::all(group_id.into());
        ListShareGroupOffsetsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds one inert caller-ordered offset operation for multiple `ShareGroups`.
    ///
    /// Each query independently selects all or explicit topic-partitions.
    /// Submission captures one public deadline and routes exact singleton
    /// requests to each group's coordinator.
    pub fn list_share_groups_offsets<I>(&self, queries: I) -> ListShareGroupsOffsetsBuilder
    where
        I: IntoIterator<Item = ListShareGroupOffsetsQuery>,
    {
        let request = ListShareGroupsOffsetsAdminRequest::new(queries.into_iter().collect());
        ListShareGroupsOffsetsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }
}
