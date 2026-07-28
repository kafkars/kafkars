//! Bounded ownership of accepted Admin `DescribeUserScramCredentials` machines and calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DescribeUserScramCredentialsEffect, DescribeUserScramCredentialsInput,
    DescribeUserScramCredentialsMachine, DescribeUserScramCredentialsPlan,
    DescribeUserScramCredentialsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeUserScramCredentialsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeUserScramCredentialsCall, DescribeUserScramCredentialsRawTerminal},
};

use super::{DescribeUserScramCredentialsHostError, DescribeUserScramCredentialsObserver};

pub(crate) const DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY: usize = 16;
pub(crate) const DESCRIBE_USER_SCRAM_CREDENTIALS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DescribeUserScramCredentialsAdmission {
    pub(crate) observer: DescribeUserScramCredentialsObserver,
    pub(crate) fault: Option<DescribeUserScramCredentialsHostError>,
}

/// One exact user selection ready for the engine's driver-admission stage.
pub(crate) struct DescribeUserScramCredentialsSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: DescribeUserScramCredentialsPlan,
    result_limit: usize,
}

impl DescribeUserScramCredentialsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DescribeUserScramCredentialsPlan,
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

pub(crate) enum DescribeUserScramCredentialsTurn {
    Idle,
    Progress,
    Submit(DescribeUserScramCredentialsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DescribeUserScramCredentialsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct DescribeUserScramCredentialsOperation {
    operation_id: OperationId,
    machine: DescribeUserScramCredentialsMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<DescribeUserScramCredentialsSubmission>,
    handoff: DescribeUserScramCredentialsHandoff,
    call: Option<DescribeUserScramCredentialsCall>,
    raw_terminal: Option<DescribeUserScramCredentialsRawTerminal>,
    terminal: Option<DescribeUserScramCredentialsTerminal>,
}

pub(crate) struct DescribeUserScramCredentialsHost {
    operations: Vec<DescribeUserScramCredentialsOperation>,
    completions: CompletionRegistry<
        DescribeUserScramCredentialsTerminal,
        AdminDescribeUserScramCredentialsPublisher,
    >,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeUserScramCredentialsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeUserScramCredentialsHost {
    pub(crate) fn new(publisher: AdminDescribeUserScramCredentialsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeUserScramCredentialsTurn, DescribeUserScramCredentialsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeUserScramCredentialsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeUserScramCredentialsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(
                operation_id,
                DescribeUserScramCredentialsInput::DeadlineElapsed,
            )?;
            return Ok(DescribeUserScramCredentialsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeUserScramCredentialsHostError::MissingSubmission)?;
        self.operations[index].handoff = DescribeUserScramCredentialsHandoff::HandedOff;
        Ok(DescribeUserScramCredentialsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeUserScramCredentialsCall,
    ) -> Result<(), DescribeUserScramCredentialsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeUserScramCredentialsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeUserScramCredentialsHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(DescribeUserScramCredentialsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(
            operation_id,
            DescribeUserScramCredentialsInput::DriverAccepted,
        )
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), DescribeUserScramCredentialsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeUserScramCredentialsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeUserScramCredentialsHandoff::HandedOff {
            return Err(DescribeUserScramCredentialsHostError::InvalidHandoff);
        }
        self.apply(
            operation_id,
            DescribeUserScramCredentialsInput::DriverRejected,
        )
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
        input: DescribeUserScramCredentialsInput,
    ) -> Result<(), DescribeUserScramCredentialsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeUserScramCredentialsHostError::UnknownOperation)?;
        let accepted = matches!(&input, DescribeUserScramCredentialsInput::DriverAccepted);
        if accepted
            && self.operations[index].handoff != DescribeUserScramCredentialsHandoff::HandedOff
        {
            return Err(DescribeUserScramCredentialsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DescribeUserScramCredentialsHandoff::Submitted;
        }
        if let Some(DescribeUserScramCredentialsEffect::Complete { terminal, .. }) =
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
