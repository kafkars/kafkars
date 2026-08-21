//! Admission handoff for public Admin `DescribeFeatures`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::describe_features::AdminDescribeFeatures;

impl AdminEngine {
    pub(crate) fn submit_describe_features(&self, timeout: Duration) -> AdminDescribeFeatures {
        AdminDescribeFeatures::from_admission(self.handle.try_describe_features(timeout))
    }
}
