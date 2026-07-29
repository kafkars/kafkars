//! Linear submission decomposition at the driver handoff seam.

use kafka_client_core::{ListConsumerGroupOffsetsPlan, OperationId};

use crate::clock::OperationDeadline;

use super::ListConsumerGroupOffsetsSubmission;

impl ListConsumerGroupOffsetsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        ListConsumerGroupOffsetsPlan,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}
