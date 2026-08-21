//! Focused engine admission for public Admin `ListOffsets`.

use std::time::Duration;

use super::{AdminListOffsets, ListOffsetsAdminRequest};
use crate::bridge::admin::AdminEngine;

impl AdminEngine {
    pub(crate) fn submit_list_offsets(
        &self,
        request: ListOffsetsAdminRequest,
        timeout: Duration,
    ) -> AdminListOffsets {
        AdminListOffsets::from_admission(
            self.handle.try_list_offsets(request.into_engine(), timeout),
        )
    }
}
