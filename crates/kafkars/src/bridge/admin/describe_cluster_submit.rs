//! Admission handoff for public Admin `DescribeCluster`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::admin_describe_operation::AdminDescribeCluster;

impl AdminEngine {
    pub(crate) fn submit_describe_cluster(&self, timeout: Duration) -> AdminDescribeCluster {
        self.submit_describe_cluster_with_options(false, false, timeout)
    }

    pub(crate) fn submit_describe_cluster_with_options(
        &self,
        include_fenced_brokers: bool,
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> AdminDescribeCluster {
        AdminDescribeCluster::from_admission(self.handle.try_describe_cluster_with_options(
            include_fenced_brokers,
            include_authorized_operations,
            timeout,
        ))
    }
}
