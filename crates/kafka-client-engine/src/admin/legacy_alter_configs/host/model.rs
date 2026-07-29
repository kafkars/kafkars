//! Operation state, prepared API 33 request, and exact driver-handoff ownership.

use kafka_client_core::{
    LegacyAlterConfigsMachine, LegacyAlterConfigsPlan, LegacyAlterConfigsRoute,
    LegacyAlterConfigsTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{
        LegacyAlterConfigsCall, LegacyAlterConfigsTerminal as DriverTerminal,
        RecoveredLegacyAlterConfigsCall,
    },
};

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

pub(super) struct LegacyAlterConfigsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: LegacyAlterConfigsMachine,
    pub(super) route: Option<LegacyAlterConfigsRoute>,
    pub(super) plan: Option<LegacyAlterConfigsPlan>,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) remaining_result_bytes: usize,
    pub(super) active_result_limit: usize,
    pub(super) active_result_contribution: usize,
    pub(super) submission: Option<LegacyAlterConfigsSubmission>,
    pub(super) handoff: LegacyAlterConfigsHandoff,
    pub(super) call: Option<LegacyAlterConfigsCall>,
    pub(super) recovered_call: Option<RecoveredLegacyAlterConfigsCall>,
    pub(super) raw_terminal: Option<DriverTerminal>,
    pub(super) terminal: Option<LegacyAlterConfigsTerminal>,
}

impl LegacyAlterConfigsOperation {
    pub(super) fn matches_correlation(
        &self,
        route: LegacyAlterConfigsRoute,
        plan: &LegacyAlterConfigsPlan,
    ) -> bool {
        self.route == Some(route) && self.plan.as_ref() == Some(plan)
    }
}
