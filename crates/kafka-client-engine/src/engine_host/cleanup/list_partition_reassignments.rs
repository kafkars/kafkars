//! Terminal quiescence check for the partition-reassignment listing owner.

use super::super::{EngineHostError, EngineHostResources};

pub(super) fn verify(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    let unsettled = resources
        .list_partition_reassignments
        .terminal_host()
        .unsettled();
    if unsettled != 0 {
        return Err(EngineHostError::ListPartitionReassignments(
            crate::admin::ListPartitionReassignmentsHostError::Unsettled(unsettled),
        ));
    }
    Ok(())
}
