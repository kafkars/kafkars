//! Prepared empty request and exact driver-handoff ownership.

use kafka_client_core::OperationId;

use crate::clock::OperationDeadline;

/// Empty API18 request ready for the later engine-host adapter.
pub(crate) struct DescribeFeaturesSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) result_limit: usize,
}

impl DescribeFeaturesSubmission {
    pub(crate) const fn into_parts(self) -> (OperationId, OperationDeadline, usize) {
        (self.operation_id, self.deadline, self.result_limit)
    }
}

pub(crate) enum DescribeFeaturesTurn {
    Idle,
    Progress,
    Submit(DescribeFeaturesSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DescribeFeaturesHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
