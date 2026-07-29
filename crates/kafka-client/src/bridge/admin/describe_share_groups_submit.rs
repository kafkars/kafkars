//! Admission handoff for public Admin `DescribeShareGroups`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::describe_share_groups::{
    AdminDescribeShareGroups, DescribeShareGroupsAdminRequest,
};

impl AdminEngine {
    pub(crate) fn submit_describe_share_groups(
        &self,
        request: DescribeShareGroupsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeShareGroups {
        AdminDescribeShareGroups::from_admission(
            self.handle
                .try_describe_share_groups(request.into_engine(), timeout),
        )
    }
}
