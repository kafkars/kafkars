//! Exact ACL-deletion operation, submission, and driver-evidence ownership.

use kafka_client_core::{
    DeleteAclFilterResult, DeleteAclsMachine, DeleteAclsPlan, DeleteAclsState, DeleteAclsTerminal,
    OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{DeleteAclsCall, DeleteAclsRawTerminal, RecoveredDeleteAclsCall},
};

use super::super::{DeleteAclsOutcome, DeleteAclsPreparedOutcomes};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DeleteAclsAttemptBounds {
    pub(super) request_limit: usize,
    pub(super) nested_count_capacity: usize,
    pub(super) result_capacity: usize,
    pub(super) outcome_capacity: usize,
}

/// One caller-positioned deletion plan ready for driver admission.
pub(crate) struct DeleteAclsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: DeleteAclsPlan,
    pub(super) bounds: DeleteAclsAttemptBounds,
}

impl DeleteAclsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DeleteAclsPlan,
        usize,
        usize,
        usize,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.bounds.request_limit,
            self.bounds.nested_count_capacity,
            self.bounds.result_capacity,
            self.bounds.outcome_capacity,
        )
    }
}

pub(crate) enum DeleteAclsTurn {
    Idle,
    Progress,
    Submit(DeleteAclsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeleteAclsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct DeleteAclsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: DeleteAclsMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) remaining_response_bytes: usize,
    pub(super) prepared_core_result_bytes: usize,
    pub(super) prepared_results: Option<Vec<DeleteAclFilterResult>>,
    pub(super) matching_counts: Vec<usize>,
    pub(super) prepared_outcomes: Option<DeleteAclsPreparedOutcomes>,
    pub(super) prepared_outcome_bytes: usize,
    pub(super) bounds: DeleteAclsAttemptBounds,
    pub(super) submission: Option<DeleteAclsSubmission>,
    pub(super) handoff: DeleteAclsHandoff,
    pub(super) call: Option<DeleteAclsCall>,
    pub(super) recovered_call: Option<RecoveredDeleteAclsCall>,
    pub(super) raw_terminal: Option<DeleteAclsRawTerminal>,
    pub(super) terminal: Option<DeleteAclsTerminal>,
    pub(super) outcome: Option<DeleteAclsOutcome>,
}

impl DeleteAclsOperation {
    pub(super) fn matches_evidence(
        &self,
        plan: &DeleteAclsPlan,
        request_limit: usize,
        nested_count_capacity: usize,
        result_capacity: usize,
        outcome_capacity: usize,
    ) -> bool {
        self.machine.state() == DeleteAclsState::AwaitingDriver
            && self.machine.plan() == Some(plan)
            && self.bounds
                == (DeleteAclsAttemptBounds {
                    request_limit,
                    nested_count_capacity,
                    result_capacity,
                    outcome_capacity,
                })
            && self.storage_matches_bounds()
    }

    pub(super) fn matches_call(&self, call: &DeleteAclsCall) -> bool {
        let Some(plan) = self.machine.plan() else {
            return false;
        };
        self.machine.state() == DeleteAclsState::AwaitingDriver
            && call.matches(
                plan,
                self.bounds.request_limit,
                self.bounds.nested_count_capacity,
                self.bounds.result_capacity,
                self.bounds.outcome_capacity,
            )
            && self.storage_matches_bounds()
    }

    pub(super) fn matches_recovered(&self, recovered: &RecoveredDeleteAclsCall) -> bool {
        let Some(plan) = self.machine.plan() else {
            return false;
        };
        matches!(
            self.machine.state(),
            DeleteAclsState::AwaitingDriver | DeleteAclsState::Submitted
        ) && recovered.matches(
            plan,
            self.bounds.request_limit,
            self.bounds.nested_count_capacity,
            self.bounds.result_capacity,
            self.bounds.outcome_capacity,
        ) && self.storage_matches_bounds()
    }

    pub(super) fn matches_raw(&self, raw: &DeleteAclsRawTerminal) -> bool {
        let Some(plan) = self.machine.plan() else {
            return false;
        };
        self.machine.state() == DeleteAclsState::Submitted
            && raw.matches(
                plan,
                self.bounds.request_limit,
                self.bounds.nested_count_capacity,
                self.bounds.result_capacity,
                self.bounds.outcome_capacity,
            )
            && self.storage_matches_bounds()
    }

    fn storage_matches_bounds(&self) -> bool {
        self.remaining_response_bytes == self.bounds.request_limit
            && self.matching_counts.capacity() == self.bounds.nested_count_capacity
            && self.prepared_results.as_ref().map(Vec::capacity)
                == Some(self.bounds.result_capacity)
            && self.prepared_core_result_bytes
                == self
                    .bounds
                    .result_capacity
                    .checked_mul(core::mem::size_of::<DeleteAclFilterResult>())
                    .unwrap_or(usize::MAX)
            && self
                .prepared_outcomes
                .as_ref()
                .map(DeleteAclsPreparedOutcomes::outcomes_capacity)
                == Some(self.bounds.outcome_capacity)
            && self
                .prepared_outcomes
                .as_ref()
                .and_then(DeleteAclsPreparedOutcomes::retained_heap_bytes)
                == Some(self.prepared_outcome_bytes)
    }
}
