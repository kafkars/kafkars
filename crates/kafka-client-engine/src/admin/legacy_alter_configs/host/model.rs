//! Prepared API 33 request and exact driver-handoff ownership.

use kafka_client_core::{LegacyAlterConfigsPlan, LegacyAlterConfigsRoute, OperationId};

use crate::clock::OperationDeadline;

/// Validated API 33 request ready for the engine-host adapter.
pub(crate) struct LegacyAlterConfigsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) route: LegacyAlterConfigsRoute,
    pub(super) plan: LegacyAlterConfigsPlan,
    pub(super) result_limit: usize,
}

impl LegacyAlterConfigsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        LegacyAlterConfigsRoute,
        LegacyAlterConfigsPlan,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.route,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum LegacyAlterConfigsTurn {
    Idle,
    Progress,
    Submit(LegacyAlterConfigsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LegacyAlterConfigsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
