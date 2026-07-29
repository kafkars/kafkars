//! Admission handoff for public Admin `AlterShareGroupOffsets`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::alter_share_group_offsets::{
    AdminAlterShareGroupOffsets, AlterShareGroupOffsetsAdminRequest,
};

impl AdminEngine {
    pub(crate) fn submit_alter_share_group_offsets(
        &self,
        request: AlterShareGroupOffsetsAdminRequest,
        timeout: Duration,
    ) -> AdminAlterShareGroupOffsets {
        AdminAlterShareGroupOffsets::from_admission(
            self.handle
                .try_alter_share_group_offsets(request.into_engine(), timeout),
        )
    }
}
