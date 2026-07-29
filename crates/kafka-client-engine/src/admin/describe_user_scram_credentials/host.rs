//! Bounded ownership of accepted Admin `DescribeUserScramCredentials` machines and calls.

mod admission;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod ownership_test;
#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DescribeUserScramCredentialsEffect, DescribeUserScramCredentialsInput,
    DescribeUserScramCredentialsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeUserScramCredentialsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::DescribeUserScramCredentialsCall,
};

use super::DescribeUserScramCredentialsHostError;

pub(crate) use model::{
    DescribeUserScramCredentialsAdmission, DescribeUserScramCredentialsSubmission,
    DescribeUserScramCredentialsTurn,
};
use model::{DescribeUserScramCredentialsHandoff, DescribeUserScramCredentialsOperation};

pub(crate) const DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY: usize = 16;
pub(crate) const DESCRIBE_USER_SCRAM_CREDENTIALS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

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
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(DescribeUserScramCredentialsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        let call = self.operations[index]
            .call
            .as_ref()
            .ok_or(DescribeUserScramCredentialsHostError::InvalidHandoff)?;
        if !self.operations[index].matches_call(call) {
            return Err(DescribeUserScramCredentialsHostError::SubmissionMismatch);
        }
        self.apply(
            operation_id,
            DescribeUserScramCredentialsInput::DriverAccepted,
        )
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: kafka_client_core::DescribeUserScramCredentialsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> Result<(), DescribeUserScramCredentialsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeUserScramCredentialsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeUserScramCredentialsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(DescribeUserScramCredentialsHostError::InvalidHandoff);
        }
        if !self.operations[index].matches_evidence(&plan, request_limit, result_limit) {
            return Err(DescribeUserScramCredentialsHostError::SubmissionMismatch);
        }
        self.apply(
            operation_id,
            DescribeUserScramCredentialsInput::DriverRejected,
        )
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
        match transition.into_effect() {
            Some(DescribeUserScramCredentialsEffect::Complete {
                operation_id: completed_id,
                terminal,
            }) if completed_id == operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)?;
            }
            Some(_) => return Err(DescribeUserScramCredentialsHostError::SubmissionMismatch),
            None => {}
        }
        Ok(())
    }
}
