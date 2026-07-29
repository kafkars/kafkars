//! Post-driver recovery for the metadata-quorum description owner.

use super::super::super::{EngineHostError, EngineHostResources};

pub(super) fn recover(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut operation = resources.describe_metadata_quorum.terminal_host();
    if let Some(cleanup) = operation
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeMetadataQuorum)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(operation);
    failure
}
