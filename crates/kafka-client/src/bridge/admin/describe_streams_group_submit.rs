//! Admission handoff for public Admin `DescribeStreamsGroup`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::describe_streams_group::{
    AdminDescribeStreamsGroup, DescribeStreamsGroupAdminRequest,
};

impl AdminEngine {
    pub(crate) fn submit_describe_streams_group(
        &self,
        request: DescribeStreamsGroupAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeStreamsGroup {
        AdminDescribeStreamsGroup::from_admission(
            self.handle
                .try_describe_streams_group(request.into_engine(), timeout),
        )
    }
}
