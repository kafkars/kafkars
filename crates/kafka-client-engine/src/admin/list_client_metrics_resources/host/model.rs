//! Prepared empty request and exact driver-handoff ownership.

use kafka_client_core::OperationId;

use crate::clock::OperationDeadline;

/// Empty API74 request ready for the later engine-host adapter.
pub(crate) struct ListClientMetricsResourcesSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) result_limit: usize,
}

impl ListClientMetricsResourcesSubmission {
    pub(crate) const fn into_parts(self) -> (OperationId, OperationDeadline, usize) {
        (self.operation_id, self.deadline, self.result_limit)
    }
}

pub(crate) enum ListClientMetricsResourcesTurn {
    Idle,
    Progress,
    Submit(ListClientMetricsResourcesSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ListClientMetricsResourcesHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
