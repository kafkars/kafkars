//! Post-driver recovery for the explicit feature-metadata owner.

use super::super::super::{EngineHostError, EngineHostResources};

pub(super) fn recover(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut describe_features = resources.describe_features.terminal_host();
    if let Some(cleanup) = describe_features
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeFeatures)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_features);
    failure
}
