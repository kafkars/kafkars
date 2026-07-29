//! Exact operation, handoff, and submission owners for finalized-feature mutation.

use kafka_client_core::{
    OperationId, UpdateFeaturesMachine, UpdateFeaturesPlan, UpdateFeaturesTerminal,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{RecoveredUpdateFeaturesCall, UpdateFeaturesCall, UpdateFeaturesRawTerminal},
};

/// Exact validated core plan ready for protocol materialization and handoff.
pub(crate) struct UpdateFeaturesSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: UpdateFeaturesPlan,
    pub(super) result_limit: usize,
}

impl UpdateFeaturesSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, UpdateFeaturesPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum UpdateFeaturesTurn {
    Idle,
    Progress,
    Submit(UpdateFeaturesSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UpdateFeaturesHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct UpdateFeaturesOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: UpdateFeaturesMachine,
    pub(super) response_plan: UpdateFeaturesPlan,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) remaining_result_bytes: usize,
    pub(super) submission: Option<UpdateFeaturesSubmission>,
    pub(super) handoff: UpdateFeaturesHandoff,
    pub(super) call: Option<UpdateFeaturesCall>,
    pub(super) recovered_call: Option<RecoveredUpdateFeaturesCall>,
    pub(super) raw_terminal: Option<UpdateFeaturesRawTerminal>,
    pub(super) terminal: Option<UpdateFeaturesTerminal>,
}
