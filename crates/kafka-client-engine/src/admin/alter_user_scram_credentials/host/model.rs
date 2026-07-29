//! Non-secret correlation plus linear prepared-request and driver ownership.

use kafka_client_core::{
    AlterUserScramCredentialsMachine, AlterUserScramCredentialsPlan,
    AlterUserScramCredentialsState, AlterUserScramCredentialsTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{
        AlterUserScramCredentialsCall, AlterUserScramCredentialsRawTerminal,
        RecoveredAlterUserScramCredentialsCall,
    },
    protocol::admin::alter_user_scram_credentials::PreparedAlterUserScramCredentialsRequest,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AlterUserScramCredentialsBounds {
    pub(super) prepared_request_bytes: usize,
    pub(super) result_limit: usize,
}

/// One non-secret plan and uniquely owned prepared secret request ready for handoff.
pub(crate) struct AlterUserScramCredentialsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: AlterUserScramCredentialsPlan,
    pub(super) prepared_request: PreparedAlterUserScramCredentialsRequest,
    pub(super) bounds: AlterUserScramCredentialsBounds,
}

impl AlterUserScramCredentialsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        AlterUserScramCredentialsPlan,
        PreparedAlterUserScramCredentialsRequest,
        usize,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.prepared_request,
            self.bounds.prepared_request_bytes,
            self.bounds.result_limit,
        )
    }
}

pub(crate) enum AlterUserScramCredentialsTurn {
    Idle,
    Progress,
    Submit(AlterUserScramCredentialsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AlterUserScramCredentialsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct AlterUserScramCredentialsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: AlterUserScramCredentialsMachine,
    pub(super) plan: AlterUserScramCredentialsPlan,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) bounds: AlterUserScramCredentialsBounds,
    pub(super) submission: Option<AlterUserScramCredentialsSubmission>,
    pub(super) rejected_submission: Option<(
        AlterUserScramCredentialsPlan,
        AlterUserScramCredentialsBounds,
    )>,
    pub(super) handoff: AlterUserScramCredentialsHandoff,
    pub(super) call: Option<AlterUserScramCredentialsCall>,
    pub(super) recovered_call: Option<RecoveredAlterUserScramCredentialsCall>,
    pub(super) raw_terminal: Option<AlterUserScramCredentialsRawTerminal>,
    pub(super) terminal: Option<AlterUserScramCredentialsTerminal>,
}

impl AlterUserScramCredentialsOperation {
    pub(super) fn matches_submission(
        &self,
        plan: &AlterUserScramCredentialsPlan,
        bounds: AlterUserScramCredentialsBounds,
    ) -> bool {
        self.machine.state() == AlterUserScramCredentialsState::AwaitingDriver
            && self.plan == *plan
            && self.bounds == bounds
    }

    pub(super) fn matches_call(&self, call: &AlterUserScramCredentialsCall) -> bool {
        matches!(
            self.machine.state(),
            AlterUserScramCredentialsState::AwaitingDriver
                | AlterUserScramCredentialsState::Submitted
        ) && call.matches_evidence(
            &self.plan,
            self.bounds.prepared_request_bytes,
            self.bounds.result_limit,
        )
    }

    pub(super) fn matches_recovered(
        &self,
        recovered: &RecoveredAlterUserScramCredentialsCall,
    ) -> bool {
        matches!(
            self.machine.state(),
            AlterUserScramCredentialsState::AwaitingDriver
                | AlterUserScramCredentialsState::Submitted
        ) && recovered.matches_evidence(
            &self.plan,
            self.bounds.prepared_request_bytes,
            self.bounds.result_limit,
        )
    }

    pub(super) fn matches_raw(&self, raw: &AlterUserScramCredentialsRawTerminal) -> bool {
        self.machine.state() == AlterUserScramCredentialsState::Submitted
            && raw.matches_evidence(
                &self.plan,
                self.bounds.prepared_request_bytes,
                self.bounds.result_limit,
            )
    }
}
