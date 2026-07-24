//! Inert `DescribeCluster` options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::admin::AdminEngine;

use super::DescribeCluster;

/// Inert broker-endpoint cluster-description request.
#[must_use = "call submit to admit the DescribeCluster operation"]
pub struct DescribeClusterBuilder {
    engine: AdminEngine,
    timeout: Duration,
    include_fenced_brokers: bool,
    include_authorized_operations: bool,
}

impl DescribeClusterBuilder {
    pub(crate) const fn new(engine: AdminEngine, timeout: Duration) -> Self {
        Self {
            engine,
            timeout,
            include_fenced_brokers: false,
            include_authorized_operations: false,
        }
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Selects whether fenced brokers are included in the cluster description.
    ///
    /// Enabling this option requires `DescribeCluster` version 2 or newer.
    pub const fn include_fenced_brokers(mut self, include: bool) -> Self {
        self.include_fenced_brokers = include;
        self
    }

    /// Selects whether Kafka should return cluster authorization bits.
    ///
    /// Kafka represents this option in every supported `DescribeCluster`
    /// version, so enabling it does not raise the operation's version floor.
    pub const fn include_authorized_operations(mut self, include: bool) -> Self {
        self.include_authorized_operations = include;
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    ///
    /// This is the public operation boundary. The engine captures its absolute
    /// deadline before admission.
    pub fn submit(self) -> DescribeCluster {
        DescribeCluster::from_bridge(self.engine.submit_describe_cluster_with_options(
            self.include_fenced_brokers,
            self.include_authorized_operations,
            self.timeout,
        ))
    }
}

impl std::fmt::Debug for DescribeClusterBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeClusterBuilder")
            .field("timeout", &self.timeout)
            .field("include_fenced_brokers", &self.include_fenced_brokers)
            .field(
                "include_authorized_operations",
                &self.include_authorized_operations,
            )
            .finish_non_exhaustive()
    }
}
