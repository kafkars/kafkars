//! Inert metadata-quorum description intent with one submission boundary.

use std::time::Duration;

use crate::bridge::admin::AdminEngine;

use super::DescribeMetadataQuorum;

/// Inert request to describe Kafka's fixed metadata quorum.
#[must_use = "call submit to admit the DescribeMetadataQuorum operation"]
pub struct DescribeMetadataQuorumBuilder {
    engine: AdminEngine,
    timeout: Duration,
}

impl DescribeMetadataQuorumBuilder {
    pub(crate) const fn new(engine: AdminEngine, timeout: Duration) -> Self {
        Self { engine, timeout }
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> DescribeMetadataQuorum {
        DescribeMetadataQuorum::from_bridge(
            self.engine.submit_describe_metadata_quorum(self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeMetadataQuorumBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeMetadataQuorumBuilder")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
