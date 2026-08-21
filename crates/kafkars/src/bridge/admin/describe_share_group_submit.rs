//! Admission handoff for public Admin `DescribeShareGroup`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::describe_share_group::{
    AdminDescribeShareGroup, DescribeShareGroupAdminRequest,
};

impl AdminEngine {
    pub(crate) fn submit_describe_share_group(
        &self,
        request: DescribeShareGroupAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeShareGroup {
        AdminDescribeShareGroup::from_admission(
            self.handle
                .try_describe_share_group(request.into_engine(), timeout),
        )
    }
}
