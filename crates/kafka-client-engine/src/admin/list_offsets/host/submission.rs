//! Linear `ListOffsets` handoff from deterministic policy to driver admission.

use kafka_client_core::{AdminListOffsetTarget, OperationId, ReadIsolation};

use crate::clock::OperationDeadline;

/// One exact target ready for the engine's driver-admission stage.
pub(crate) struct AdminListOffsetsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) target: AdminListOffsetTarget,
    pub(super) read_isolation: ReadIsolation,
}

impl AdminListOffsetsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        AdminListOffsetTarget,
        ReadIsolation,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.target,
            self.read_isolation,
        )
    }
}
