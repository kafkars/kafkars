//! Bounded ownership of accepted SCRAM credential-alteration machines and calls.

mod admission;
mod model;
#[cfg(test)]
mod ownership_test;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    AlterUserScramCredentialsEffect, AlterUserScramCredentialsInput,
    AlterUserScramCredentialsTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminAlterUserScramCredentialsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::AlterUserScramCredentialsCall,
};

use super::{AlterUserScramCredentialsHostError, AlterUserScramCredentialsObserver};
use model::{
    AlterUserScramCredentialsBounds, AlterUserScramCredentialsHandoff,
    AlterUserScramCredentialsOperation,
};
pub(crate) use model::{AlterUserScramCredentialsSubmission, AlterUserScramCredentialsTurn};

pub(crate) const ALTER_USER_SCRAM_CREDENTIALS_CAPACITY: usize = 16;
pub(crate) const ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AlterUserScramCredentialsAdmission {
    pub(crate) observer: AlterUserScramCredentialsObserver,
    pub(crate) fault: Option<AlterUserScramCredentialsHostError>,
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
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(AlterUserScramCredentialsHostError::InvalidHandoff);
        }
        let matches = self.operations[index].matches_call(&call);
        self.operations[index].call = Some(call);
        if !matches {
            return Err(AlterUserScramCredentialsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, AlterUserScramCredentialsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: kafka_client_core::AlterUserScramCredentialsPlan,
        prepared_request_bytes: usize,
        result_limit: usize,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterUserScramCredentialsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AlterUserScramCredentialsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(AlterUserScramCredentialsHostError::InvalidHandoff);
        }
        let bounds = AlterUserScramCredentialsBounds {
            prepared_request_bytes,
            result_limit,
        };
        let matches = self.operations[index].matches_submission(&plan, bounds);
        self.operations[index].rejected_submission = Some((plan, bounds));
        if !matches {
            return Err(AlterUserScramCredentialsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index]
            .machine
            .apply(AlterUserScramCredentialsInput::DriverRejected)?;
        let terminal = match transition.into_effect() {
            Some(AlterUserScramCredentialsEffect::Complete {
                operation_id: effect_id,
                terminal,
            }) if effect_id == operation_id => terminal,
            _ => return Err(AlterUserScramCredentialsHostError::MissingTerminal),
        };
        drop(self.operations[index].rejected_submission.take());
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
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
        if let Some(AlterUserScramCredentialsEffect::Complete {
            operation_id: effect_id,
            terminal,
        }) = transition.into_effect()
        {
            if effect_id != operation_id {
                return Err(AlterUserScramCredentialsHostError::SubmissionMismatch);
            }
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }
}
