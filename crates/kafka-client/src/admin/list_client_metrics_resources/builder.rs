//! Inert client-metrics resource listing intent with one submission boundary.

use std::time::Duration;

use crate::bridge::admin::AdminEngine;

use super::ListClientMetricsResources;

/// Inert request to list Kafka client-metrics configuration resources.
#[must_use = "call submit to admit the ListClientMetricsResources operation"]
pub struct ListClientMetricsResourcesBuilder {
    engine: AdminEngine,
    timeout: Duration,
}

impl ListClientMetricsResourcesBuilder {
    pub(crate) const fn new(engine: AdminEngine, timeout: Duration) -> Self {
        Self { engine, timeout }
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> ListClientMetricsResources {
        ListClientMetricsResources::from_bridge(
            self.engine
                .submit_list_client_metrics_resources(self.timeout),
        )
    }
}

impl std::fmt::Debug for ListClientMetricsResourcesBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListClientMetricsResourcesBuilder")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
