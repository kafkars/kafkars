//! Post-driver recovery for the finalized-feature mutation owner.

use super::super::super::{EngineHostError, EngineHostResources};

pub(super) fn recover(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut update_features = resources.update_features.terminal_host();
    if let Some(cleanup) = update_features
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::UpdateFeatures)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(update_features);
    failure
}
