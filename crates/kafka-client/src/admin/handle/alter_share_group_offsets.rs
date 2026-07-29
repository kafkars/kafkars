//! ShareGroup offset-alteration entry point on the shared admin handle.

use super::Admin;
use crate::{
    admin::{AlterShareGroupOffsetsBuilder, ShareGroupOffsetAlteration},
    bridge::alter_share_group_offsets::AlterShareGroupOffsetsAdminRequest,
};

impl Admin {
    /// Builds inert caller-ordered partition-offset alterations for one ShareGroup.
    ///
    /// Group, topic-partition, and offset validation remains deferred until
    /// [`AlterShareGroupOffsetsBuilder::submit`] captures the public absolute
    /// deadline and attempts bounded engine admission.
    pub fn alter_share_group_offsets<I>(
        &self,
        group_id: impl Into<String>,
        alterations: I,
    ) -> AlterShareGroupOffsetsBuilder
    where
        I: IntoIterator<Item = ShareGroupOffsetAlteration>,
    {
        let request = AlterShareGroupOffsetsAdminRequest::new(
            group_id.into(),
            alterations.into_iter().collect(),
        );
        AlterShareGroupOffsetsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }
}
