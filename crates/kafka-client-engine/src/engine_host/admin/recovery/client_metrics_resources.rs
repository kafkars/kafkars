//! Post-driver recovery for the client-metrics resource listing owner.

use super::super::super::{EngineHostError, EngineHostResources};

pub(super) fn recover(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut list_client_metrics_resources = resources.list_client_metrics_resources.terminal_host();
    if let Some(cleanup) = list_client_metrics_resources
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ListClientMetricsResources)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(list_client_metrics_resources);
    failure
}
