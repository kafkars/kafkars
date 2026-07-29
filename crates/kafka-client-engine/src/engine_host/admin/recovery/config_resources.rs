//! Post-driver recovery for the configuration-resource listing owner.

use super::super::super::{EngineHostError, EngineHostResources};

pub(super) fn recover(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut list_config_resources = resources.list_config_resources.terminal_host();
    if let Some(cleanup) = list_config_resources
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ListConfigResources)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(list_config_resources);
    failure
}
