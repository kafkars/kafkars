//! Terminal verification for partition-reassignment alterations.

use super::super::{EngineHostError, EngineHostResources};

pub(super) fn verify(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    let unsettled = resources
        .alter_partition_reassignments
        .terminal_host()
        .unsettled();
    if unsettled != 0 {
        return Err(EngineHostError::AlterPartitionReassignments(
            crate::admin::AlterPartitionReassignmentsHostError::Unsettled(unsettled),
        ));
    }
    Ok(())
}
