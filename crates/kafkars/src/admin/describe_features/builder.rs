//! Inert Kafka feature discovery intent with one submission boundary.

use std::time::Duration;

use crate::bridge::admin::AdminEngine;

use super::DescribeFeatures;

/// Inert request to discover supported and finalized Kafka features.
#[must_use = "call submit to admit the DescribeFeatures operation"]
pub struct DescribeFeaturesBuilder {
    engine: AdminEngine,
    timeout: Duration,
}

impl DescribeFeaturesBuilder {
    pub(crate) const fn new(engine: AdminEngine, timeout: Duration) -> Self {
        Self { engine, timeout }
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> DescribeFeatures {
        DescribeFeatures::from_bridge(self.engine.submit_describe_features(self.timeout))
    }
}

impl std::fmt::Debug for DescribeFeaturesBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeFeaturesBuilder")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
