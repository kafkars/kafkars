//! Exact operation, submission, handoff, and driver-evidence ownership.

use kafka_client_core::{
    AlterClientQuotasMachine, AlterClientQuotasPlan, AlterClientQuotasState,
    AlterClientQuotasTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{AlterClientQuotasCall, AlterClientQuotasRawTerminal, RecoveredAlterClientQuotasCall},
};

/// One exact alteration plan and retained limit ready for driver admission.
pub(crate) struct AlterClientQuotasSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: AlterClientQuotasPlan,
    pub(super) retained_limit: usize,
}

impl AlterClientQuotasSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (OperationId, OperationDeadline, AlterClientQuotasPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.retained_limit,
        )
    }
}

pub(crate) enum AlterClientQuotasTurn {
    Idle,
    Progress,
    Submit(AlterClientQuotasSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AlterClientQuotasHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct AlterClientQuotasOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: AlterClientQuotasMachine,
    pub(super) plan: AlterClientQuotasPlan,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) retained_limit: usize,
    pub(super) submission: Option<AlterClientQuotasSubmission>,
    pub(super) rejected_submission: Option<(AlterClientQuotasPlan, usize)>,
    pub(super) handoff: AlterClientQuotasHandoff,
    pub(super) call: Option<AlterClientQuotasCall>,
    pub(super) recovered_call: Option<RecoveredAlterClientQuotasCall>,
    pub(super) raw_terminal: Option<AlterClientQuotasRawTerminal>,
    pub(super) terminal: Option<AlterClientQuotasTerminal>,
}

impl AlterClientQuotasOperation {
    pub(super) fn matches_submission(
        &self,
        plan: &AlterClientQuotasPlan,
        retained_limit: usize,
    ) -> bool {
        self.machine.state() == AlterClientQuotasState::AwaitingDriver
            && self.plan == *plan
            && self.retained_limit == retained_limit
    }

    pub(super) fn matches_call(&self, call: &AlterClientQuotasCall) -> bool {
        matches!(
            self.machine.state(),
            AlterClientQuotasState::AwaitingDriver | AlterClientQuotasState::Submitted
        ) && call.matches(&self.plan, self.retained_limit)
    }

    pub(super) fn matches_recovered(&self, recovered: &RecoveredAlterClientQuotasCall) -> bool {
        matches!(
            self.machine.state(),
            AlterClientQuotasState::AwaitingDriver | AlterClientQuotasState::Submitted
        ) && recovered.matches(&self.plan, self.retained_limit)
    }

    pub(super) fn matches_raw(&self, raw: &AlterClientQuotasRawTerminal) -> bool {
        self.machine.state() == AlterClientQuotasState::Submitted
            && raw.matches(&self.plan, self.retained_limit)
    }
}
