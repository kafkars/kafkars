//! Admission handoff for public Admin `DeleteShareGroupOffsets`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::delete_share_group_offsets::{
    AdminDeleteShareGroupOffsets, DeleteShareGroupOffsetsAdminRequest,
};

impl AdminEngine {
    pub(crate) fn submit_delete_share_group_offsets(
        &self,
        request: DeleteShareGroupOffsetsAdminRequest,
        timeout: Duration,
    ) -> AdminDeleteShareGroupOffsets {
        AdminDeleteShareGroupOffsets::from_admission(
            self.handle
                .try_delete_share_group_offsets(request.into_engine(), timeout),
        )
    }
}
