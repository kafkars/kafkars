//! `ShareGroup` offset-deletion entry point on the shared admin handle.

use super::Admin;
use crate::{
    admin::DeleteShareGroupOffsetsBuilder,
    bridge::delete_share_group_offsets::DeleteShareGroupOffsetsAdminRequest,
};

impl Admin {
    /// Builds inert caller-ordered topic offset deletion for one `ShareGroup`.
    ///
    /// Group and topic validation remains deferred until
    /// [`DeleteShareGroupOffsetsBuilder::submit`] captures the public absolute
    /// deadline and attempts bounded engine admission.
    pub fn delete_share_group_offsets<I, T>(
        &self,
        group_id: impl Into<String>,
        topics: I,
    ) -> DeleteShareGroupOffsetsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let request = DeleteShareGroupOffsetsAdminRequest::new(
            group_id.into(),
            topics.into_iter().map(Into::into).collect(),
        );
        DeleteShareGroupOffsetsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }
}
