//! Admission handoff for public Admin `ListShareGroupOffsets`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::list_share_group_offsets::{
    AdminListShareGroupOffsets, AdminListShareGroupsOffsets, ListShareGroupOffsetsAdminRequest,
    ListShareGroupsOffsetsAdminRequest,
};

impl AdminEngine {
    pub(crate) fn submit_list_share_group_offsets(
        &self,
        request: ListShareGroupOffsetsAdminRequest,
        timeout: Duration,
    ) -> AdminListShareGroupOffsets {
        AdminListShareGroupOffsets::from_admission(
            self.handle
                .try_list_share_group_offsets(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_list_share_groups_offsets(
        &self,
        request: ListShareGroupsOffsetsAdminRequest,
        timeout: Duration,
    ) -> AdminListShareGroupsOffsets {
        AdminListShareGroupsOffsets::from_admission(
            self.handle
                .try_list_share_groups_offsets(request.into_engine(), timeout),
        )
    }
}
