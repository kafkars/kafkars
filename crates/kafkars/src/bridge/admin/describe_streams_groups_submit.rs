//! Admission handoff for public Admin `DescribeStreamsGroups`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::describe_streams_groups::{
    AdminDescribeStreamsGroups, DescribeStreamsGroupsAdminRequest,
};

impl AdminEngine {
    pub(crate) fn submit_describe_streams_groups(
        &self,
        request: DescribeStreamsGroupsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeStreamsGroups {
        AdminDescribeStreamsGroups::from_admission(
            self.handle
                .try_describe_streams_groups(request.into_engine(), timeout),
        )
    }
}
