//! Admission handoff for public Admin `DescribeCluster`.

use std::time::{Duration, Instant};

use super::AdminEngine;
use crate::bridge::admin_describe_operation::AdminDescribeCluster;

impl AdminEngine {
    pub(crate) fn submit_describe_cluster_until(&self, deadline: Instant) -> AdminDescribeCluster {
        AdminDescribeCluster::from_admission(self.handle.try_describe_cluster_until(deadline))
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
