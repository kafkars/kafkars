//! Test-only observations of retained Admin `ListClientMetricsResources` call ownership.

use super::super::{ListClientMetricsResourcesHost, ListClientMetricsResourcesHostError};

impl ListClientMetricsResourcesHost {
    pub(in crate::admin::list_client_metrics_resources) fn retain_recovered_call_for_test(
        &mut self,
    ) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredListClientMetricsResourcesCall::for_test());
    }

    pub(in crate::admin::list_client_metrics_resources) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::list_client_metrics_resources) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), ListClientMetricsResourcesHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::list_client_metrics_resources) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), ListClientMetricsResourcesHostError> {
        self.publish_terminal(0)
    }
}
