//! Admission handoff for public Admin `ListClientMetricsResources`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::list_client_metrics_resources::AdminListClientMetricsResources;

impl AdminEngine {
    pub(crate) fn submit_list_client_metrics_resources(
        &self,
        timeout: Duration,
    ) -> AdminListClientMetricsResources {
        AdminListClientMetricsResources::from_admission(
            self.handle.try_list_client_metrics_resources(timeout),
        )
    }
}
