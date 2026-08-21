//! Client-metrics configuration-resource entry point on the shared admin handle.

use super::Admin;
use crate::admin::ListClientMetricsResourcesBuilder;

impl Admin {
    /// Builds inert intent to list client-metrics configuration resources.
    ///
    /// This initial surface deliberately uses Kafka API 74 v0 semantics on
    /// every compatible broker. No timeout starts and no operation is admitted
    /// until [`ListClientMetricsResourcesBuilder::submit`] is called.
    pub fn list_client_metrics_resources(&self) -> ListClientMetricsResourcesBuilder {
        ListClientMetricsResourcesBuilder::new(self.engine.clone(), self.engine.default_timeout())
    }
}
