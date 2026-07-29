//! Bounded ownership of accepted SCRAM credential-alteration machines and calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    AlterUserScramCredentialsEffect, AlterUserScramCredentialsInput,
    AlterUserScramCredentialsMachine, AlterUserScramCredentialsPlan,
    AlterUserScramCredentialsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminAlterUserScramCredentialsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{AlterUserScramCredentialsCall, AlterUserScramCredentialsRawTerminal},
    protocol::admin::alter_user_scram_credentials::PreparedAlterUserScramCredentialsRequest,
};

use super::{AlterUserScramCredentialsHostError, AlterUserScramCredentialsObserver};

pub(crate) const ALTER_USER_SCRAM_CREDENTIALS_CAPACITY: usize = 16;
pub(crate) const ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AlterUserScramCredentialsAdmission {
    pub(crate) observer: AlterUserScramCredentialsObserver,
    pub(crate) fault: Option<AlterUserScramCredentialsHostError>,
}

/// One non-secret plan and prepared API-key 51 owner ready for handoff.
pub(crate) struct AlterUserScramCredentialsSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: AlterUserScramCredentialsPlan,
    prepared_request: PreparedAlterUserScramCredentialsRequest,
}

impl AlterUserScramCredentialsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        AlterUserScramCredentialsPlan,
        PreparedAlterUserScramCredentialsRequest,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.prepared_request,
        )
    }
}

pub(crate) enum AlterUserScramCredentialsTurn {
    Idle,
    Progress,
    Submit(AlterUserScramCredentialsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AlterUserScramCredentialsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct AlterUserScramCredentialsOperation {
    operation_id: OperationId,
    machine: AlterUserScramCredentialsMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<AlterUserScramCredentialsSubmission>,
    handoff: AlterUserScramCredentialsHandoff,
    call: Option<AlterUserScramCredentialsCall>,
    raw_terminal: Option<AlterUserScramCredentialsRawTerminal>,
    terminal: Option<AlterUserScramCredentialsTerminal>,
}

pub(crate) struct AlterUserScramCredentialsHost {
    operations: Vec<AlterUserScramCredentialsOperation>,
    completions: CompletionRegistry<
        AlterUserScramCredentialsTerminal,
        AdminAlterUserScramCredentialsPublisher,
    >,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AlterUserScramCredentialsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AlterUserScramCredentialsHost {
    pub(crate) fn new(publisher: AdminAlterUserScramCredentialsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ALTER_USER_SCRAM_CREDENTIALS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ALTER_USER_SCRAM_CREDENTIALS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ALTER_USER_SCRAM_CREDENTIALS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AlterUserScramCredentialsTurn, AlterUserScramCredentialsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AlterUserScramCredentialsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AlterUserScramCredentialsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(
                operation_id,
                AlterUserScramCredentialsInput::DeadlineElapsed,
            )?;
            return Ok(AlterUserScramCredentialsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AlterUserScramCredentialsHostError::MissingSubmission)?;
        self.operations[index].handoff = AlterUserScramCredentialsHandoff::HandedOff;
        Ok(AlterUserScramCredentialsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: AlterUserScramCredentialsCall,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterUserScramCredentialsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AlterUserScramCredentialsHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(AlterUserScramCredentialsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, AlterUserScramCredentialsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterUserScramCredentialsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AlterUserScramCredentialsHandoff::HandedOff {
            return Err(AlterUserScramCredentialsHostError::InvalidHandoff);
        }
        self.apply(operation_id, AlterUserScramCredentialsInput::DriverRejected)
    }

    pub(crate) fn close_admission(&mut self) {
        self.accepting = false;
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.operations.len()
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.operations
            .iter()
            .filter(|operation| operation.submission.is_some())
            .map(|operation| operation.deadline.core())
            .min()
    }

    fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .iter()
            .position(|operation| operation.operation_id == operation_id)
    }

    fn apply(
        &mut self,
        operation_id: OperationId,
        input: AlterUserScramCredentialsInput,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterUserScramCredentialsHostError::UnknownOperation)?;
        let accepted = matches!(&input, AlterUserScramCredentialsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AlterUserScramCredentialsHandoff::HandedOff
        {
            return Err(AlterUserScramCredentialsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AlterUserScramCredentialsHandoff::Submitted;
        }
        if let Some(AlterUserScramCredentialsEffect::Complete { terminal, .. }) =
            transition.into_effect()
        {
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
